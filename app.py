#Author: Manan Tarun Shah - Production Technology

import os
import re
import requests
import threading
import concurrent.futures
import csv
from tkinter import filedialog
import customtkinter as ctk
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Callable, Optional
from dotenv import load_dotenv
from sqlglot import parse_one, exp
from sqlglot.errors import ParseError

# --- UI Appearance Settings ---
ctk.set_appearance_mode("Dark")  # Options: "Dark", "Light", "System"
ctk.set_default_color_theme("blue")

class GrafanaLineageMapper:
    """Core Logic Engine: Handles API calls and SQL parsing."""
    
    GRAFANA_MACRO_PATTERNS = [
        (re.compile(r"\$__time\w*\([^\)]*\)"), "CURRENT_TIMESTAMP"),
        (re.compile(r"\$__interval\w*"), "'1h'"),
        (re.compile(r"\$\{[^\}]+\}"), "'dummy'"),
        (re.compile(r"\$\w+"), "'dummy'"),
    ]
    
    SQL_TABLE_PATTERNS = [
        re.compile(r'(?:FROM|JOIN)\s+([A-Za-z0-9_]+\.[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)?)', re.IGNORECASE),
        re.compile(r'"([A-Za-z0-9_]+)"\."([A-Za-z0-9_]+)"(?:\."([A-Za-z0-9_]+)")?', re.IGNORECASE),
    ]

    def __init__(self, grafana_url: str, api_token: str):
        self.grafana_url = grafana_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        })
        
        # Structure 1: table_name -> { dashboard_string: set( (panel_or_variable_names, raw_sql) ) }
        self.table_to_dashboards_map = defaultdict(lambda: defaultdict(set))
        # Structure 2: dashboard_string -> { panel_or_variable_name: set(table_names) }
        self.dashboard_to_tables_map = defaultdict(lambda: defaultdict(set))

    def fetch_dashboard_metadata(self, target_folders: Optional[List[str]] = None) -> List[Tuple[str, str, str]]:
        """Fetches a list of (uid, title, folderTitle) for dashboards. If target_folders is None/empty, fetches all."""
        print(f"DEBUG: Fetching dashboards from Grafana URL: {self.grafana_url}")
        response = self.session.get(f"{self.grafana_url}/api/search?type=dash-db")
        response.raise_for_status()
        all_dashboards = response.json()
        
        # NORMALIZE: If no specific folders are requested, return all of them
        if not target_folders:
            filtered_dashboards = [
                (dash['uid'], dash['title'], dash.get('folderTitle', 'General')) 
                for dash in all_dashboards 
            ]
            print(f"DEBUG: Processing all {len(filtered_dashboards)} dashboards across all folders.")
            return filtered_dashboards

        target_folders_set = {folder.upper() for folder in target_folders}
        filtered_dashboards = [
            (dash['uid'], dash['title'], dash.get('folderTitle', 'General')) 
            for dash in all_dashboards 
            if dash.get('folderTitle', '').upper() in target_folders_set
        ]
        
        print(f"DEBUG: Filtered down to {len(filtered_dashboards)} dashboards based on target folders: {target_folders_set}")
        return filtered_dashboards

    def fetch_dashboard_json(self, dashboard_uid: str) -> Dict:
        """Fetches the full JSON definition for a specific dashboard."""
        try:
            url = f"{self.grafana_url}/api/dashboards/uid/{dashboard_uid}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json().get('dashboard', {})
        except requests.RequestException as e:
            print(f"DEBUG: API Error fetching dashboard UID {dashboard_uid}: {e}")
            return {}

    def extract_table_names_from_sql(self, raw_sql_query: str) -> Set[str]:
        """Cleans Grafana macros from the query and attempts to extract Snowflake table names."""
        cleaned_sql = raw_sql_query
        for pattern, replacement in self.GRAFANA_MACRO_PATTERNS:
            cleaned_sql = pattern.sub(replacement, cleaned_sql)
        
        extracted_tables = set()
        
        try:
            parsed_tree = parse_one(cleaned_sql, read='snowflake')
            
            # Identify all CTE names (e.g., "WITH KY_ODL AS...") to avoid flagging them as tables
            cte_names = {cte.alias.upper() for cte in parsed_tree.find_all(exp.CTE) if cte.alias}

            for table_node in parsed_tree.find_all(exp.Table):
                # If a table has no database/schema prefix AND its name matches a known CTE, skip it.
                if not table_node.db and not table_node.catalog and table_node.name.upper() in cte_names:
                    continue
                    
                # NORMALIZE: Drop the catalog (database) and only keep schema and table name
                sql_parts = [part for part in (table_node.db, table_node.name) if part]
                if sql_parts:
                    extracted_tables.add('.'.join(sql_parts).upper())
            if extracted_tables: 
                return extracted_tables
        except ParseError:
            pass
            
        for regex_pattern in self.SQL_TABLE_PATTERNS:
            regex_matches = regex_pattern.findall(cleaned_sql)
            for match in regex_matches:
                table_string = '.'.join(filter(None, match)) if isinstance(match, tuple) else match
                if table_string: 
                    # Normalize regex matches to a maximum of 2 parts (Schema.Table)
                    parts = table_string.split('.')
                    normalized_string = '.'.join(parts[-2:]).upper()
                    extracted_tables.add(normalized_string)
                    
        return extracted_tables

    def build_dependency_maps(self, target_folders: Optional[List[str]] = None, progress_callback: Optional[Callable] = None) -> None:
        """Iterates through dashboards and populates both directional dependency maps."""
        dashboard_list = self.fetch_dashboard_metadata(target_folders)
        total_dashboards = len(dashboard_list)
        if total_dashboards == 0: 
            print("DEBUG: 0 dashboards to process. Exiting build process.")
            return

        for _, title, folder in dashboard_list:
            formatted_name = f"[{folder}] {title}"
            _ = self.dashboard_to_tables_map[formatted_name]

        def analyze_dashboard_queries(dashboard_info):
            dash_uid, dash_title, dash_folder = dashboard_info
            dashboard_json = self.fetch_dashboard_json(dash_uid)
            extracted_queries_with_context = []
            
            for template_var in dashboard_json.get('templating', {}).get('list', []):
                query_definition = template_var.get('query')
                variable_name = template_var.get('name', 'Unknown')
                ui_source_label = f"Variable: {variable_name}"
                
                if isinstance(query_definition, str): 
                    extracted_queries_with_context.append((query_definition, ui_source_label))
                elif isinstance(query_definition, dict) and 'rawSql' in query_definition: 
                    extracted_queries_with_context.append((query_definition['rawSql'], ui_source_label))
            
            def recursively_extract_panel_queries(panels_list):
                for panel in panels_list:
                    panel_title = panel.get('title', 'Untitled Panel')
                    ui_source_label = f"Panel: {panel_title}"
                    
                    for target in panel.get('targets', []):
                        if 'rawSql' in target: 
                            extracted_queries_with_context.append((target['rawSql'], ui_source_label))
                    if 'panels' in panel: 
                        recursively_extract_panel_queries(panel['panels'])
            
            recursively_extract_panel_queries(dashboard_json.get('panels', []))
            
            analyzed_results = []
            formatted_dashboard_name = f"[{dash_folder}] {dash_title}"
            
            for sql_query, ui_source_label in extracted_queries_with_context:
                for table_name in self.extract_table_names_from_sql(sql_query):
                    analyzed_results.append((table_name, formatted_dashboard_name, ui_source_label, sql_query))
            
            return analyzed_results

        dashboards_completed = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as thread_executor:
            pending_futures = {thread_executor.submit(analyze_dashboard_queries, dash): dash for dash in dashboard_list}
            
            for future in concurrent.futures.as_completed(pending_futures):
                try:
                    for table_name, dashboard_name, source_label, sql_query in future.result(timeout=15):
                        self.table_to_dashboards_map[table_name][dashboard_name].add((source_label, sql_query))
                        self.dashboard_to_tables_map[dashboard_name][source_label].add(table_name)
                except Exception as e:
                    failed_dash = pending_futures[future]
                    print(f"DEBUG: Failed to process dashboard '{failed_dash[1]}' (UID: {failed_dash[0]}) - Error: {e}")
                
                dashboards_completed += 1
                if progress_callback:
                    progress_callback(dashboards_completed / total_dashboards, dashboards_completed, total_dashboards)

class FlakeLensApp(ctk.CTk):
    """The Desktop GUI Application."""
    def __init__(self):
        super().__init__()
        self.title("FlakeLens")
        self.geometry("950x700")

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        # State Preservation Variables
        self.current_mode = "Find Dashboards from Table"
        self.search_text_state = {
            "Find Dashboards from Table": "",
            "Find Tables from Dashboard": ""
        }
        
        # UI Tracking Variables
        self.search_text_var = ctk.StringVar()
        self.column_input_var = ctk.StringVar()
        self.suggestion_frame = None
        self.is_selecting_suggestion = False  
        self.is_loading = False 
        
        # Export Tracking Variables
        self.current_search_results = []
        self.current_searched_term = ""
        
        # Caching lists for fast autocomplete
        self.all_dashboard_names = []
        self.all_table_names = []

        # --- Sidebar ---
        self.sidebar_frame = ctk.CTkFrame(self, width=220, corner_radius=0)
        self.sidebar_frame.grid(row=0, column=0, sticky="nsew")
        
        self.logo_label = ctk.CTkLabel(self.sidebar_frame, text="FlakeLens", font=ctk.CTkFont(size=28, weight="bold"))
        self.logo_label.grid(row=0, column=0, padx=20, pady=(30, 30))
        
        self.status_header = ctk.CTkLabel(self.sidebar_frame, text="SYSTEM STATUS", font=ctk.CTkFont(size=14, weight="bold"))
        self.status_header.grid(row=1, column=0, padx=20, sticky="w")
        
        self.status_indicator_label = ctk.CTkLabel(self.sidebar_frame, text="⠋ Loading...", text_color="#FFCC00")
        self.status_indicator_label.grid(row=2, column=0, padx=20, pady=(0, 20), sticky="w")

        # --- Quick Guide Section ---
        self.guide_header = ctk.CTkLabel(self.sidebar_frame, text="QUICK GUIDE", font=ctk.CTkFont(size=14, weight="bold"))
        self.guide_header.grid(row=3, column=0, padx=20, pady=(10, 5), sticky="w")

        guide_instructions = (
            "1. Wait for the status above to\n"
            "   turn Green (Connected).\n\n"
            "2. Select your search mode using\n"
            "   the toggle switch.\n\n"
            "3. Enter a Table or Dashboard\n"
            "   name and press Search.\n\n"
            "4. Click the ▶ arrows to expand\n"
            "   the results."
        )

        self.guide_label = ctk.CTkLabel(
            self.sidebar_frame, 
            text=guide_instructions, 
            font=ctk.CTkFont(size=12),
            text_color="gray75",
            justify="left"
        )
        self.guide_label.grid(row=4, column=0, padx=20, sticky="w")

        # --- Note Section ---
        self.note_header = ctk.CTkLabel(self.sidebar_frame, text="NOTE:", font=ctk.CTkFont(size=14, weight="bold"))
        self.note_header.grid(row=5, column=0, padx=20, pady=(20,5), sticky="w")

        note_instructions = (
            "All Snowflake tables will be\n" 
            "displayed in the format SCHEMA.TABLE,\n" 
            "even if they are in the format\n" 
            "DATABASE.SCHEMA.TABLE in the \n"
            "grafana panel query."
        )
        
        self.note_label = ctk.CTkLabel(
            self.sidebar_frame, 
            text=note_instructions, 
            font=ctk.CTkFont(size=12),
            text_color="gray75",
            justify="left"
        )
        self.note_label.grid(row=6, column=0, padx=20, sticky="w")

        # --- Main Content ---
        self.main_content_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.main_content_frame.grid(row=0, column=1, padx=30, pady=30, sticky="nsew")
        self.main_content_frame.grid_columnconfigure(0, weight=1)

        # Header 
        self.main_header_label = ctk.CTkLabel(
            self.main_content_frame, 
            text="FLAKELENS : Give me a dashboard name, I'll find you all the tables in it.\nGive me a table name, I'll find you all the dashboards using it.", 
            font=ctk.CTkFont(size=16, weight="bold"),
            justify="left"
        )
        self.main_header_label.grid(row=0, column=0, sticky="w", pady=(0, 5))

        # Tip
        self.tip_label = ctk.CTkLabel(
            self.main_content_frame,
            text="💡 Tip: This tool is restricted to look at dashboards only from PRODUCTION and ProdTech folders under Grafana.",
            font=ctk.CTkFont(size=12, slant="italic"),
            text_color="gray60",
            justify="left"
        )
        self.tip_label.grid(row=1, column=0, sticky="w", pady=(0, 15))

        # Mode Selector Toggle
        self.search_mode_toggle = ctk.CTkSegmentedButton(
            self.main_content_frame, 
            values=["Find Dashboards from Table", "Find Tables from Dashboard"],
            command=self.handle_search_mode_switch
        )
        self.search_mode_toggle.grid(row=2, column=0, sticky="ew", pady=(0, 20))
        self.search_mode_toggle.set(self.current_mode)

        # --- Dynamic Search Bar Area ---
        self.search_input_container = ctk.CTkFrame(self.main_content_frame, fg_color="transparent")
        self.search_input_container.grid(row=3, column=0, sticky="ew", pady=(0, 10))
        self.search_input_container.grid_columnconfigure(1, weight=1)

        # Row 0: Table / Dashboard Primary Search
        self.search_label = ctk.CTkLabel(self.search_input_container, text="Table Name:", font=ctk.CTkFont(weight="bold", size=13))
        self.search_label.grid(row=0, column=0, padx=(0, 10), sticky="e")

        self.search_input_entry = ctk.CTkEntry(
            self.search_input_container, 
            textvariable=self.search_text_var,
            height=45
        )
        self.search_input_entry.grid(row=0, column=1, sticky="ew", padx=(0, 10))
        self.search_input_entry.bind("<Return>", self.process_search_request)
        self.search_input_entry.configure(state="disabled")

        self.execute_search_button = ctk.CTkButton(self.search_input_container, text="Search", height=45, width=90, command=self.process_search_request, state="disabled")
        self.execute_search_button.grid(row=0, column=2, padx=(0, 10))

        self.export_csv_button = ctk.CTkButton(self.search_input_container, text="Export CSV", height=45, width=90, command=self.export_to_csv, state="disabled", fg_color="#27AE60", hover_color="#2ECC71")
        self.export_csv_button.grid(row=0, column=3)

        # Row 2: Column Search (Row 1 reserved for dynamic dropdown)
        self.column_label = ctk.CTkLabel(self.search_input_container, text="Lookup by Column (Optional):", font=ctk.CTkFont(weight="bold", size=13))
        self.column_label.grid(row=2, column=0, padx=(0, 10), pady=(10, 0), sticky="e")

        self.column_input_entry = ctk.CTkEntry(
            self.search_input_container, 
            textvariable=self.column_input_var,
            height=35
        )
        self.column_input_entry.grid(row=2, column=1, sticky="ew", padx=(0, 10), pady=(10, 0))
        self.column_input_entry.bind("<Return>", self.process_search_request)

        self.grid_placeholder = ctk.CTkLabel(self.search_input_container, text="", width=90)
        self.grid_placeholder.grid(row=2, column=3)

        # Attach the live typing event listener
        self.search_text_var.trace_add("write", self.handle_typing_suggestions)

        # --- ENHANCED LOADING UI ---
        self.loading_container = ctk.CTkFrame(self.main_content_frame, fg_color="transparent")
        self.loading_container.grid(row=4, column=0, sticky="ew", pady=(20, 10))
        self.loading_container.grid_columnconfigure(0, weight=1)

        self.loading_title = ctk.CTkLabel(self.loading_container, text="Syncing Grafana & Snowflake Lineage...", font=ctk.CTkFont(size=14, weight="bold"))
        self.loading_title.grid(row=0, column=0, sticky="w", pady=(0, 8))

        self.loading_progress_bar = ctk.CTkProgressBar(self.loading_container, height=12, progress_color="#2ECC71") 
        self.loading_progress_bar.grid(row=1, column=0, sticky="ew")
        self.loading_progress_bar.set(0)

        self.loading_legends_frame = ctk.CTkFrame(self.loading_container, fg_color="transparent")
        self.loading_legends_frame.grid(row=2, column=0, sticky="ew", pady=(5, 0))
        self.loading_legends_frame.grid_columnconfigure(1, weight=1)

        self.loading_percent_label = ctk.CTkLabel(self.loading_legends_frame, text="0%", font=ctk.CTkFont(size=13, weight="bold"), text_color="#2ECC71")
        self.loading_percent_label.grid(row=0, column=0, sticky="w")

        self.loading_detail_label = ctk.CTkLabel(self.loading_legends_frame, text="0 / 0 dashboards processed", font=ctk.CTkFont(size=12), text_color="gray60")
        self.loading_detail_label.grid(row=0, column=1, sticky="e")

        # State Preservation Frames
        self.results_frame_dashboards = ctk.CTkScrollableFrame(self.main_content_frame)
        self.results_frame_tables = ctk.CTkScrollableFrame(self.main_content_frame)
        
        self.main_content_frame.grid_rowconfigure(5, weight=1)
        
        self.active_results_frame = self.results_frame_dashboards
        self.active_results_frame.grid(row=5, column=0, sticky="nsew", pady=(10, 0))

        # Bind global click to detect background clicks
        self.bind_all("<Button-1>", self.handle_background_click)

        # Start mapping thread and animation loop
        self.is_loading = True
        self.animate_loading_spinner()
        threading.Thread(target=self.initialize_and_build_maps, daemon=True).start()

    def export_to_csv(self):
        """Prompts the user to save the current search results to a CSV file."""
        if not self.current_search_results:
            return

        safe_term = re.sub(r'[\\/*?:"<>|]', "", self.current_searched_term)
        default_filename = f"FlakeLens_Export_{safe_term}.csv"

        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            title="Save Search Results",
            initialfile=default_filename
        )

        if not file_path:
            return

        try:
            with open(file_path, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if self.current_mode == "Find Dashboards from Table":
                    writer.writerow(["Table Name", "Dashboard Name", "Panel/Variable Name"])
                else:
                    writer.writerow(["Dashboard Name", "Panel/Variable Name", "Table Name"])
                
                unique_results = sorted(list(set(tuple(row) for row in self.current_search_results)))
                writer.writerows(unique_results)
        except Exception as e:
            print(f"DEBUG: Failed to export CSV: {e}")

    def animate_loading_spinner(self, frame_index=0):
        if not self.is_loading:
            return  
        spinners = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        self.status_indicator_label.configure(text=f"{spinners[frame_index]} Loading...", text_color="#FFCC00")
        self.after(80, self.animate_loading_spinner, (frame_index + 1) % len(spinners))

    def handle_typing_suggestions(self, *args):
        """Monitors keystrokes and displays dropdown suggestions."""
        if self.is_selecting_suggestion:
            return  

        self.hide_suggestion_frame()
        raw_search_term = self.search_text_var.get().strip().upper()

        if len(raw_search_term) < 2:
            return

        if self.current_mode == "Find Dashboards from Table":
            parts = raw_search_term.split('.')
            search_term = '.'.join(parts[-2:])
            matches = [t for t in self.all_table_names if search_term in t]
        else:
            search_term = raw_search_term
            matches = [d for d in self.all_dashboard_names if search_term in d.upper()]

        if not matches:
            return

        matches = matches[:10]

        # BUILD DROPDOWN
        self.suggestion_frame = ctk.CTkScrollableFrame(
            self.search_input_container, 
            height=min(len(matches) * 35, 200),
            fg_color=("gray90", "gray15"),
            corner_radius=4
        )
        self.suggestion_frame.grid(row=1, column=1, sticky="ew", padx=(0, 10), pady=(2, 0))

        for match in matches:
            btn = ctk.CTkButton(
                self.suggestion_frame,
                text=match,
                anchor="w",
                fg_color="transparent",
                text_color=("black", "white"),
                hover_color=("gray80", "gray25"),
                corner_radius=0,
                command=lambda m=match: self.select_suggestion(m)
            )
            btn.pack(fill="x", pady=1)

    def select_suggestion(self, match: str):
        """Populates the search bar and cleans up UI without auto-searching."""
        self.is_selecting_suggestion = True
        self.search_text_var.set(match)
        self.after(50, self._finalize_selection)

    def _finalize_selection(self):
        """Cleans up the UI after a suggestion is selected."""
        self.hide_suggestion_frame()
        self.is_selecting_suggestion = False

    def hide_suggestion_frame(self):
        if hasattr(self, 'suggestion_frame') and self.suggestion_frame is not None:
            try:
                self.suggestion_frame.grid_forget() 
                self.suggestion_frame.destroy()
            except Exception:
                pass
            self.suggestion_frame = None

    def handle_background_click(self, event):
        if not hasattr(self, 'suggestion_frame') or self.suggestion_frame is None:
            return
        clicked_widget_str = str(event.widget)
        entry_widget_str = str(self.search_input_entry)
        suggestion_frame_str = str(self.suggestion_frame)

        if not clicked_widget_str.startswith(entry_widget_str) and not clicked_widget_str.startswith(suggestion_frame_str):
            self.hide_suggestion_frame()
            self.focus_set()

    def handle_search_mode_switch(self, selected_mode: str):
        self.hide_suggestion_frame()
        
        if not selected_mode:
            selected_mode = self.current_mode
            self.search_mode_toggle.set(selected_mode)
            
        if selected_mode == self.current_mode:
            return 
            
        self.search_text_state[self.current_mode] = self.search_text_var.get()
        self.current_mode = selected_mode
        self.active_results_frame.grid_forget() 
        
        if self.current_mode == "Find Dashboards from Table":
            self.search_label.configure(text="Table Name:")
            self.active_results_frame = self.results_frame_dashboards
            
            # Show Column search controls
            self.column_label.grid(row=2, column=0, padx=(0, 10), pady=(10, 0), sticky="e")
            self.column_input_entry.grid(row=2, column=1, sticky="ew", padx=(0, 10), pady=(10, 0))
            self.grid_placeholder.grid(row=2, column=3)
            
        else:
            self.search_label.configure(text="Dashboard Name:")
            self.active_results_frame = self.results_frame_tables
            
            # Hide Column search controls safely
            self.column_label.grid_remove()
            self.column_input_entry.grid_remove()
            self.grid_placeholder.grid_remove()
            self.column_input_var.set("") # Clear column search state
            
        self.active_results_frame.grid(row=5, column=0, sticky="nsew", pady=(10, 0))
        
        self.is_selecting_suggestion = True
        self.search_input_entry.delete(0, 'end') 
        saved_text = self.search_text_state[self.current_mode]
        if saved_text:
            self.search_input_entry.insert(0, saved_text)
            
        self.is_selecting_suggestion = False
        self.search_input_entry.focus()
        
        self.current_search_results = []
        self.export_csv_button.configure(state="disabled")

    def initialize_and_build_maps(self):
        load_dotenv()
        grafana_url = os.getenv("GRAFANA_URL")
        api_token = os.getenv("GRAFANA_TOKEN")
        
        if not grafana_url or not api_token:
            def _handle_missing_creds():
                self.is_loading = False
                self.status_indicator_label.configure(text="● Missing Credentials", text_color="#E74C3C")
            self.after(0, _handle_missing_creds)
            return

        try:
            self.lineage_mapper = GrafanaLineageMapper(grafana_url, api_token)
            self.lineage_mapper.build_dependency_maps(["ProdTech", "PRODUCTION"], self.update_loading_progress)
            
            self.all_table_names = sorted(list(self.lineage_mapper.table_to_dashboards_map.keys()))
            self.all_dashboard_names = sorted(list(self.lineage_mapper.dashboard_to_tables_map.keys()))
            
            self.after(0, self.enable_user_inputs)
        except Exception as e:
            def _handle_fatal_error():
                self.is_loading = False
                self.status_indicator_label.configure(text="● Connection Failed", text_color="#E74C3C")
            self.after(0, _handle_fatal_error)

    def update_loading_progress(self, progress_ratio: float, current_count: int, total_count: int):
        def _update_ui():
            percentage = int(progress_ratio * 100)
            self.loading_progress_bar.set(progress_ratio)
            self.loading_percent_label.configure(text=f"{percentage}%")
            self.loading_detail_label.configure(text=f"{current_count} / {total_count} dashboards processed")
        self.after(0, _update_ui)

    def enable_user_inputs(self):
        self.loading_container.grid_forget()
        self.is_loading = False
        self.status_indicator_label.configure(text="● Connected", text_color="#2ECC71")
        
        self.search_input_entry.configure(state="normal")
        self.execute_search_button.configure(state="normal")
        self.search_input_entry.focus()

    def process_search_request(self, event=None):
        self.hide_suggestion_frame()
        raw_search_term = self.search_text_var.get().strip().upper()
        
        self.current_search_results = []
        self.current_searched_term = raw_search_term
        self.export_csv_button.configure(state="disabled")
        
        if self.current_mode == "Find Dashboards from Table" and raw_search_term:
            parts = raw_search_term.split('.')
            search_term = '.'.join(parts[-2:])
        else:
            search_term = raw_search_term

        for widget in self.active_results_frame.winfo_children():
            widget.destroy()
            
        if not search_term: 
            return

        if self.current_mode == "Find Dashboards from Table":
            
            requested_cols = []
            standard_col_patterns = []
            has_star_request = False
            raw_col_input = self.column_input_var.get().strip()
            
            # Smart default: If raw_col_input exists, validate and build patterns.
            if raw_col_input:
                # STRICT VALIDATION: Look for items securely enclosed in single quotes
                extracted_quotes = re.findall(r"'([^']+)'", raw_col_input)
                
                if not extracted_quotes:
                    error_str = "Error: Invalid column format.\n\nColumn names must be wrapped in single quotes and separated by commas.\nExample: 'COL_A', '*'"
                    empty_msg = ctk.CTkLabel(self.active_results_frame, text=error_str, text_color="#E74C3C", justify="left")
                    empty_msg.pack(pady=20, padx=10, anchor="w")
                    return
                
                requested_cols = [c.strip().upper() for c in extracted_quotes if c.strip()]
                
                # Pre-compile Standard Regex Patterns
                for col in requested_cols:
                    if col == '*':
                        has_star_request = True
                    else:
                        standard_col_patterns.append(re.compile(r'\b' + re.escape(col) + r'\b', re.IGNORECASE))
            
            matched_dashboards_with_sources = defaultdict(set)
            
            for table_name, dashboards_dict in self.lineage_mapper.table_to_dashboards_map.items():
                if search_term in table_name or table_name.endswith(f".{search_term}"):
                    
                    # Dynamically build the strictly bound star pattern for this specific table
                    star_pattern = None
                    if has_star_request:
                        base_table = table_name.split('.')[-1]
                        # Regex: Match SELECT * but bounded by the target table without crossing into another SELECT block
                        regex_str = r'\bSELECT\b(?:(?!\bFROM\b).)*?(?:[a-zA-Z0-9_]+\.)?\*(?:(?!\bSELECT\b).)*?\b' + re.escape(base_table) + r'\b'
                        star_pattern = re.compile(regex_str, re.IGNORECASE | re.DOTALL)
                        
                    for dashboard_name, sources_and_sqls in dashboards_dict.items():
                        
                        for source_label, sql_query in sources_and_sqls:
                            # If columns were requested, filter by them
                            if requested_cols:
                                # Clean the SQL: Strip block (/* */), inline (--), AND Aggregates like COUNT(*)
                                clean_sql = re.sub(r'/\*.*?\*/', '', sql_query, flags=re.DOTALL)
                                clean_sql = re.sub(r'--.*', '', clean_sql)
                                clean_sql = re.sub(r'\b\w+\s*\(\s*\*\s*\)', 'FUNC_STAR', clean_sql) # Purges COUNT(*) etc.
                                
                                match_found = False
                                
                                # Check standard columns (OR Logic)
                                if standard_col_patterns and any(p.search(clean_sql) for p in standard_col_patterns):
                                    match_found = True
                                # Check perfectly bounded star match 
                                elif star_pattern and star_pattern.search(clean_sql):
                                    match_found = True
                                    
                                if not match_found:
                                    continue 
                            
                            matched_dashboards_with_sources[dashboard_name].add(source_label)
                            self.current_search_results.append([table_name, dashboard_name, source_label])
            
            if not matched_dashboards_with_sources:
                if requested_cols:
                    msg_str = f"No dashboards use this column.\n\n(Searched for: {', '.join(requested_cols)})"
                    empty_msg = ctk.CTkLabel(self.active_results_frame, text=msg_str, justify="left")
                else:
                    empty_msg = ctk.CTkLabel(self.active_results_frame, text=f"No dependencies found matching '{search_term}'.")
                    
                empty_msg.pack(pady=20, padx=10, anchor="w")
                return
                
            results_header = ctk.CTkLabel(self.active_results_frame, text=f"Found {len(matched_dashboards_with_sources)} dashboard(s) using '{search_term}':", font=ctk.CTkFont(weight="bold"))
            results_header.pack(pady=(10, 10), padx=10, anchor="w")
            
            for dashboard_name in sorted(matched_dashboards_with_sources.keys()):
                ui_sources_list = sorted(matched_dashboards_with_sources[dashboard_name])
                self._render_table_search_result_accordion(self.active_results_frame, dashboard_name, ui_sources_list)

            if self.current_search_results:
                self.export_csv_button.configure(state="normal")

        else:
            matched_dashboards_with_tables = {}
            for dashboard_name, sources_to_tables_dict in self.lineage_mapper.dashboard_to_tables_map.items():
                if search_term in dashboard_name.upper():
                    matched_dashboards_with_tables[dashboard_name] = sources_to_tables_dict
                    for panel_name, table_list in sources_to_tables_dict.items():
                        for table_name in table_list:
                            self.current_search_results.append([dashboard_name, panel_name, table_name])
            
            if not matched_dashboards_with_tables:
                empty_msg = ctk.CTkLabel(self.active_results_frame, text=f"No dashboards found matching '{search_term}'.")
                empty_msg.pack(pady=20, padx=10, anchor="w")
                return
            
            results_header = ctk.CTkLabel(self.active_results_frame, text=f"Found {len(matched_dashboards_with_tables)} dashboard(s) matching '{search_term}':", font=ctk.CTkFont(weight="bold"))
            results_header.pack(pady=(10, 10), padx=10, anchor="w")

            for dashboard_name, sources_to_tables_dict in sorted(matched_dashboards_with_tables.items()):
                self._render_dashboard_search_result_accordion(self.active_results_frame, dashboard_name, sources_to_tables_dict)
                
            if self.current_search_results:
                self.export_csv_button.configure(state="normal")

    def _render_table_search_result_accordion(self, parent_frame, dashboard_title, ui_sources):
        accordion_container = ctk.CTkFrame(parent_frame, fg_color="transparent")
        accordion_container.pack(fill="x", padx=5, pady=2)
        
        details_panel = ctk.CTkFrame(accordion_container, fg_color=("gray85", "gray20"), corner_radius=5)
        
        def toggle_accordion_state():
            if details_panel.winfo_ismapped():
                details_panel.pack_forget()
                expand_button.configure(text=f"▶ {dashboard_title}")
            else:
                details_panel.pack(fill="x", pady=(2, 5), padx=(20, 0))
                expand_button.configure(text=f"▼ {dashboard_title}")

        expand_button = ctk.CTkButton(
            accordion_container, 
            text=f"▶ {dashboard_title}", 
            anchor="w", 
            fg_color=("gray75", "gray25"), 
            text_color=("black", "white"), 
            hover_color=("gray70", "gray30"), 
            command=toggle_accordion_state
        )
        expand_button.pack(fill="x")
        
        for source_item in ui_sources:
            item_label = ctk.CTkLabel(details_panel, text=f"• {source_item}", anchor="w", font=ctk.CTkFont(size=12))
            item_label.pack(fill="x", padx=10, pady=2)

    def _render_dashboard_search_result_accordion(self, parent_frame, dashboard_title, sources_to_tables_dict):
        dashboard_container = ctk.CTkFrame(parent_frame, fg_color="transparent")
        dashboard_container.pack(fill="x", padx=5, pady=2)
        
        dashboard_details_panel = ctk.CTkFrame(dashboard_container, fg_color=("gray85", "gray20"), corner_radius=5)
        
        def toggle_dashboard_state():
            if dashboard_details_panel.winfo_ismapped():
                dashboard_details_panel.pack_forget()
                dashboard_button.configure(text=f"▶ {dashboard_title}")
            else:
                dashboard_details_panel.pack(fill="x", pady=(2, 5), padx=(20, 0))
                dashboard_button.configure(text=f"▼ {dashboard_title}")

        dashboard_button = ctk.CTkButton(
            dashboard_container, 
            text=f"▶ {dashboard_title}", 
            anchor="w", 
            fg_color=("gray75", "gray25"), 
            text_color=("black", "white"), 
            hover_color=("gray70", "gray30"), 
            command=toggle_dashboard_state
        )
        dashboard_button.pack(fill="x")

        if not sources_to_tables_dict:
            empty_lbl = ctk.CTkLabel(dashboard_details_panel, text="No Snowflake tables detected in this dashboard.", font=ctk.CTkFont(slant="italic"), text_color="gray60")
            empty_lbl.pack(padx=10, pady=10, anchor="w")
            return
        
        for panel_name, table_list in sorted(sources_to_tables_dict.items()):
            panel_container = ctk.CTkFrame(dashboard_details_panel, fg_color="transparent")
            panel_container.pack(fill="x", padx=10, pady=2)

            tables_display_panel = ctk.CTkFrame(panel_container, fg_color="transparent")

            def create_panel_toggle(target_frame, target_button, button_text):
                def _execute_toggle():
                    if target_frame.winfo_ismapped():
                        target_frame.pack_forget()
                        target_button.configure(text=f"▶ {button_text}")
                    else:
                        target_frame.pack(fill="x", pady=(0, 5), padx=(20, 0))
                        target_button.configure(text=f"▼ {button_text}")
                return _execute_toggle

            panel_button = ctk.CTkButton(
                panel_container, 
                text=f"▶ {panel_name}", 
                anchor="w", 
                fg_color=("gray80", "gray35"), 
                text_color=("black", "white"), 
                hover_color=("gray75", "gray40")
            )
            panel_button.configure(command=create_panel_toggle(tables_display_panel, panel_button, panel_name))
            panel_button.pack(fill="x")
            
            for table_name in sorted(table_list):
                table_label = ctk.CTkLabel(tables_display_panel, text=f"↳ {table_name}", anchor="w", font=ctk.CTkFont(size=12, family="Consolas"))
                table_label.pack(fill="x", padx=5, pady=1)

if __name__ == "__main__":
    app = FlakeLensApp()
    app.mainloop()