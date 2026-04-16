#Author: Manan Tarun Shah - Production Technology
#Co-author: Gemini

import os
import re
import requests
import threading
import concurrent.futures
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
        
        # Structure 1: table_name -> { dashboard_string: set(panel_or_variable_names) }
        self.table_to_dashboards_map = defaultdict(lambda: defaultdict(set))
        # Structure 2: dashboard_string -> { panel_or_variable_name: set(table_names) }
        self.dashboard_to_tables_map = defaultdict(lambda: defaultdict(set))

    def fetch_dashboard_metadata(self, target_folders: List[str]) -> List[Tuple[str, str, str]]:
        """Fetches a list of (uid, title, folderTitle) for dashboards in target folders."""
        print(f"DEBUG: Fetching dashboards from Grafana URL: {self.grafana_url}")
        response = self.session.get(f"{self.grafana_url}/api/search?type=dash-db")
        response.raise_for_status()
        all_dashboards = response.json()
        
        print(f"DEBUG: Found {len(all_dashboards)} total dashboards in Grafana.")
        
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
                # This merges US_OPS_ANALYTICS.DELIVERY_LIST.ODL_ER down to DELIVERY_LIST.ODL_ER
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

    def build_dependency_maps(self, target_folders: List[str], progress_callback: Optional[Callable] = None) -> None:
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
                    analyzed_results.append((table_name, formatted_dashboard_name, ui_source_label))
            
            return analyzed_results

        dashboards_completed = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as thread_executor:
            pending_futures = {thread_executor.submit(analyze_dashboard_queries, dash): dash for dash in dashboard_list}
            
            for future in concurrent.futures.as_completed(pending_futures):
                try:
                    for table_name, dashboard_name, source_label in future.result(timeout=15):
                        self.table_to_dashboards_map[table_name][dashboard_name].add(source_label)
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
        self.suggestion_frame = None
        self.is_selecting_suggestion = False  # Prevents infinite loops when a suggestion is clicked
        self.is_loading = False # Flags for animation
        
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

        self.note_header = ctk.CTkLabel(self.sidebar_frame, text="NOTE:", font=ctk.CTkFont(size=14, weight="bold"))
        self.note_header.grid(row=5, column=0, padx=20, pady=(20,5), sticky="w")

        note_instructions = ("All Snowflake tables will be\n" 
                             "displayed in the format SCHEMA.TABLE,\n" 
                             "even if they are in the format\n" 
                             "DATABASE.SCHEMA.TABLE in the \n"
                             "grafana panel query.")
        
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

        # Search Bar Area
        self.search_input_container = ctk.CTkFrame(self.main_content_frame, fg_color="transparent")
        self.search_input_container.grid(row=3, column=0, sticky="ew", pady=(0, 10))
        self.search_input_container.grid_columnconfigure(0, weight=1)

        # The entry field mapped to the string var
        self.search_input_entry = ctk.CTkEntry(
            self.search_input_container, 
            textvariable=self.search_text_var, 
            height=45
        )
        self.search_input_entry.grid(row=0, column=0, sticky="ew", padx=(0, 10))
        self.search_input_entry.bind("<Return>", self.process_search_request)
        self.search_input_entry.configure(state="disabled")

        self.execute_search_button = ctk.CTkButton(self.search_input_container, text="Search", height=45, command=self.process_search_request, state="disabled")
        self.execute_search_button.grid(row=0, column=1)

        # Attach the live typing event listener for suggestions
        self.search_text_var.trace_add("write", self.handle_typing_suggestions)

        # --- ENHANCED LOADING UI ---
        self.loading_container = ctk.CTkFrame(self.main_content_frame, fg_color="transparent")
        self.loading_container.grid(row=4, column=0, sticky="ew", pady=(20, 10))
        self.loading_container.grid_columnconfigure(0, weight=1)

        # Title
        self.loading_title = ctk.CTkLabel(self.loading_container, text="Syncing Grafana & Snowflake Lineage...", font=ctk.CTkFont(size=14, weight="bold"))
        self.loading_title.grid(row=0, column=0, sticky="w", pady=(0, 8))

        # Thicker, greener progress bar
        self.loading_progress_bar = ctk.CTkProgressBar(self.loading_container, height=12, progress_color="#2ECC71") 
        self.loading_progress_bar.grid(row=1, column=0, sticky="ew")
        self.loading_progress_bar.set(0)

        # Container for the legends (Percentage left, Count right)
        self.loading_legends_frame = ctk.CTkFrame(self.loading_container, fg_color="transparent")
        self.loading_legends_frame.grid(row=2, column=0, sticky="ew", pady=(5, 0))
        self.loading_legends_frame.grid_columnconfigure(1, weight=1)

        # 0% text
        self.loading_percent_label = ctk.CTkLabel(self.loading_legends_frame, text="0%", font=ctk.CTkFont(size=13, weight="bold"), text_color="#2ECC71")
        self.loading_percent_label.grid(row=0, column=0, sticky="w")

        # 0 / 0 text
        self.loading_detail_label = ctk.CTkLabel(self.loading_legends_frame, text="0 / 0 dashboards processed", font=ctk.CTkFont(size=12), text_color="gray60")
        self.loading_detail_label.grid(row=0, column=1, sticky="e")

        # State Preservation Frames
        self.results_frame_dashboards = ctk.CTkScrollableFrame(self.main_content_frame)
        self.results_frame_tables = ctk.CTkScrollableFrame(self.main_content_frame)
        
        self.main_content_frame.grid_rowconfigure(5, weight=1)
        
        self.active_results_frame = self.results_frame_dashboards
        self.active_results_frame.grid(row=5, column=0, sticky="nsew", pady=(10, 0))

        # --- Bind global click to detect background clicks ---
        self.bind_all("<Button-1>", self.handle_background_click)

        # Start mapping thread and animation loop
        self.is_loading = True
        self.animate_loading_spinner()
        threading.Thread(target=self.initialize_and_build_maps, daemon=True).start()

    def animate_loading_spinner(self, frame_index=0):
        """Creates a text-based animation loop for the sidebar loading status."""
        if not self.is_loading:
            return  # Stop the loop if loading has finished or failed
            
        spinners = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        self.status_indicator_label.configure(text=f"{spinners[frame_index]} Loading...", text_color="#FFCC00")
        
        # Schedule the next frame in 80 milliseconds
        self.after(80, self.animate_loading_spinner, (frame_index + 1) % len(spinners))

    def handle_typing_suggestions(self, *args):
        """Monitors keystrokes and displays relevant dropdown suggestions."""
        if self.is_selecting_suggestion:
            return  # Ignore the trace if the code itself is setting the text

        self.hide_suggestion_frame()
        raw_search_term = self.search_text_var.get().strip().upper()

        # Require at least 2 characters to trigger autocomplete
        if len(raw_search_term) < 2:
            return

        # Determine which list to filter
        if self.current_mode == "Find Dashboards from Table":
            # NORMALIZE USER INPUT: Drop DB prefix for autocomplete filtering
            parts = raw_search_term.split('.')
            search_term = '.'.join(parts[-2:])
            matches = [t for t in self.all_table_names if search_term in t]
        else:
            search_term = raw_search_term
            matches = [d for d in self.all_dashboard_names if search_term in d.upper()]

        if not matches:
            return

        # Cap at top 15 results to prevent CustomTkinter from lagging
        matches = matches[:10]

        # Build the dynamic suggestion dropdown frame
        self.suggestion_frame = ctk.CTkScrollableFrame(
            self.search_input_container, 
            height=min(len(matches) * 35, 200), # Auto-size height based on content
            fg_color=("gray90", "gray15"),
            corner_radius=4
        )
        # Place it strictly under the input bar in column 0
        self.suggestion_frame.grid(row=1, column=0, sticky="ew", padx=(0, 10), pady=(2, 0))

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
        """Populates the search bar and executes the search when a suggestion is clicked."""
        # Turn on the flag to prevent the typing trace from firing
        self.is_selecting_suggestion = True
        self.search_text_var.set(match)
        
        # Delay the destruction of the frame by 50ms so the button's click event can finish cleanly
        self.after(50, self._finalize_selection)

    def _finalize_selection(self):
        """Executes the search and cleans up the UI after a short delay."""
        self.hide_suggestion_frame()
        self.process_search_request()
        # Safe to unlock the typing trace again
        self.is_selecting_suggestion = False

    def hide_suggestion_frame(self):
        """Cleans up and destroys the temporary suggestion box."""
        if hasattr(self, 'suggestion_frame') and self.suggestion_frame is not None:
            try:
                # Always remove from the grid BEFORE destroying to prevent visual glitches
                self.suggestion_frame.grid_forget() 
                self.suggestion_frame.destroy()
            except Exception:
                pass
            self.suggestion_frame = None

    def handle_background_click(self, event):
        """Detects clicks outside the search entry and suggestion pane to close suggestions."""
        if not hasattr(self, 'suggestion_frame') or self.suggestion_frame is None:
            return

        # Get the underlying tk widget string paths
        clicked_widget_str = str(event.widget)
        entry_widget_str = str(self.search_input_entry)
        suggestion_frame_str = str(self.suggestion_frame)

        # If the clicked widget is NOT the entry bar and NOT part of the suggestion frame
        if not clicked_widget_str.startswith(entry_widget_str) and not clicked_widget_str.startswith(suggestion_frame_str):
            self.hide_suggestion_frame()
            # Optionally set focus to the main app window to fully clear the entry focus
            self.focus_set()

    def handle_search_mode_switch(self, selected_mode: str):
        """Swaps active frames and manages state preservation when toggling modes."""
        self.hide_suggestion_frame()
        
        if not selected_mode:
            selected_mode = self.current_mode
            self.search_mode_toggle.set(selected_mode)
            
        if selected_mode == self.current_mode:
            return 
            
        # 1. Save current text state
        self.search_text_state[self.current_mode] = self.search_text_var.get()
        self.current_mode = selected_mode
        self.active_results_frame.grid_forget() 
        
        # 2. Swap result frames
        if self.current_mode == "Find Dashboards from Table":
            self.active_results_frame = self.results_frame_dashboards
        else:
            self.active_results_frame = self.results_frame_tables
            
        self.active_results_frame.grid(row=5, column=0, sticky="nsew", pady=(10, 0))
        
        # 3. Restore text state for the newly selected mode
        self.is_selecting_suggestion = True
        
        # Explicitly delete native contents to clear the field
        self.search_input_entry.delete(0, 'end') 
        
        # Insert saved text if there is any
        saved_text = self.search_text_state[self.current_mode]
        if saved_text:
            self.search_input_entry.insert(0, saved_text)
            
        self.is_selecting_suggestion = False
            
        self.search_input_entry.focus()

    def initialize_and_build_maps(self):
        """Loads environment variables and builds the lineage maps in the background."""
        load_dotenv()
        grafana_url = os.getenv("GRAFANA_URL")
        api_token = os.getenv("GRAFANA_TOKEN")
        
        if not grafana_url or not api_token:
            print("DEBUG: Missing URL or Token in .env file.")
            def _handle_missing_creds():
                self.is_loading = False
                self.status_indicator_label.configure(text="● Missing Credentials", text_color="#E74C3C")
            self.after(0, _handle_missing_creds)
            return

        try:
            self.lineage_mapper = GrafanaLineageMapper(grafana_url, api_token)
            self.lineage_mapper.build_dependency_maps(["ProdTech", "PRODUCTION"], self.update_loading_progress)
            
            # Cache the full lists for fast autocomplete filtering
            self.all_table_names = sorted(list(self.lineage_mapper.table_to_dashboards_map.keys()))
            self.all_dashboard_names = sorted(list(self.lineage_mapper.dashboard_to_tables_map.keys()))
            
            self.after(0, self.enable_user_inputs)
            print(f"DEBUG: Indexing complete! Tables mapped: {len(self.all_table_names)}")
            print(f"DEBUG: Dashboards mapped: {len(self.all_dashboard_names)}")
        except Exception as e:
            print(f"DEBUG: Fatal error during initialization: {e}")
            def _handle_fatal_error():
                self.is_loading = False
                self.status_indicator_label.configure(text="● Connection Failed", text_color="#E74C3C")
            self.after(0, _handle_fatal_error)

    def update_loading_progress(self, progress_ratio: float, current_count: int, total_count: int):
        """Safely updates the GUI progress bar from the background worker thread."""
        def _update_ui():
            # Calculate integer percentage
            percentage = int(progress_ratio * 100)
            
            # Update the physical bar
            self.loading_progress_bar.set(progress_ratio)
            
            # Update the rich UI text labels
            self.loading_percent_label.configure(text=f"{percentage}%")
            self.loading_detail_label.configure(text=f"{current_count} / {total_count} dashboards processed")
            
        self.after(0, _update_ui)

    def enable_user_inputs(self):
        """Unlocks the GUI once map building is complete."""
        # Hide the entire enhanced loading block
        self.loading_container.grid_forget()
        
        # Kill the animation and set the label to connected
        self.is_loading = False
        self.status_indicator_label.configure(text="● Connected", text_color="#2ECC71")
        
        self.search_input_entry.configure(state="normal")
        self.execute_search_button.configure(state="normal")
        self.search_input_entry.focus()

    def process_search_request(self, event=None):
        """Routes the search logic to the currently active frame."""
        self.hide_suggestion_frame()
        
        raw_search_term = self.search_text_var.get().strip().upper()
        
        # NORMALIZE USER INPUT: Strip DB prefix before searching
        if self.current_mode == "Find Dashboards from Table" and raw_search_term:
            parts = raw_search_term.split('.')
            search_term = '.'.join(parts[-2:])
        else:
            search_term = raw_search_term

        print(f"DEBUG: Executing Search | Mode: '{self.current_mode}' | Raw: '{raw_search_term}' | Normalized: '{search_term}'")
        
        # Clear ONLY the currently active results frame before populating
        for widget in self.active_results_frame.winfo_children():
            widget.destroy()
            
        if not search_term: 
            return

        if self.current_mode == "Find Dashboards from Table":
            # --- MODE 1: Table -> Dashboards -> Panels ---
            matched_dashboards_with_sources = defaultdict(set)
            
            for table_name, dashboards_dict in self.lineage_mapper.table_to_dashboards_map.items():
                if search_term in table_name or table_name.endswith(f".{search_term}"):
                    for dashboard_name, ui_sources in dashboards_dict.items():
                        matched_dashboards_with_sources[dashboard_name].update(ui_sources)
            
            if not matched_dashboards_with_sources:
                empty_msg = ctk.CTkLabel(self.active_results_frame, text=f"No dependencies found matching '{search_term}'.")
                empty_msg.pack(pady=20, padx=10, anchor="w")
                return
                
            results_header = ctk.CTkLabel(self.active_results_frame, text=f"Found {len(matched_dashboards_with_sources)} dashboard(s) using '{search_term}':", font=ctk.CTkFont(weight="bold"))
            results_header.pack(pady=(10, 10), padx=10, anchor="w")
            
            for dashboard_name in sorted(matched_dashboards_with_sources.keys()):
                ui_sources_list = sorted(matched_dashboards_with_sources[dashboard_name])
                self._render_table_search_result_accordion(self.active_results_frame, dashboard_name, ui_sources_list)

        else:
            # --- MODE 2: Dashboard -> Panels -> Tables (Nested) ---
            matched_dashboards_with_tables = {}
            
            for dashboard_name, sources_to_tables_dict in self.lineage_mapper.dashboard_to_tables_map.items():
                if search_term in dashboard_name.upper():
                    matched_dashboards_with_tables[dashboard_name] = sources_to_tables_dict
            
            if not matched_dashboards_with_tables:
                empty_msg = ctk.CTkLabel(self.active_results_frame, text=f"No dashboards found matching '{search_term}'.")
                empty_msg.pack(pady=20, padx=10, anchor="w")
                return
            
            results_header = ctk.CTkLabel(self.active_results_frame, text=f"Found {len(matched_dashboards_with_tables)} dashboard(s) matching '{search_term}':", font=ctk.CTkFont(weight="bold"))
            results_header.pack(pady=(10, 10), padx=10, anchor="w")

            for dashboard_name, sources_to_tables_dict in sorted(matched_dashboards_with_tables.items()):
                self._render_dashboard_search_result_accordion(self.active_results_frame, dashboard_name, sources_to_tables_dict)

    def _render_table_search_result_accordion(self, parent_frame, dashboard_title, ui_sources):
        """Creates a standard 1-level expandable accordion (Dashboard -> Panels)."""
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
        """Creates a 2-level expandable accordion (Dashboard -> Panel -> Tables)."""
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