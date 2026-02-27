from click import prompt
import pandas as pd
from langgraph.graph import StateGraph, START, END
from pydantic import BaseModel
import ollama
import json
import ast


class CleaningState(BaseModel):
    input_text: str
    structured_response: str = ""


class AIAgent:
    def __init__(self):
        self.graph = self.create_graph()

    def create_graph(self):
        graph = StateGraph(CleaningState)

        def agent_logic(state: CleaningState) -> CleaningState:
            # Call the clean_data function and update the state
            response = ollama.chat(
                model="llama3:latest",   # or whichever model you downloaded in Ollama
                messages=[{"role": "user", "content": state.input_text}]
            )

            # Robust way to extract text from Ollama response
            if isinstance(response, list):
                # Ollama returns list of messages in older versions
                state.structured_response = response[0]['content']
            elif hasattr(response, "messages"):
                # Ollama returns ChatResponse object in newer versions
                state.structured_response = response.messages[0].content
            else:
                # fallback
                state.structured_response = str(response)

            return CleaningState(
                input_text=state.input_text,
                structured_response=state.structured_response
            )

        # Define the cleaning node
        graph.add_node("clean_data", agent_logic)

        # Connect the nodes
        graph.add_edge("clean_data", END)

        # Set entry point
        graph.set_entry_point("clean_data")

        return graph.compile()

    def _clean_batch(self, batch_df):
        # Convert batch to string (important)
        batch_text = batch_df.to_string()

        prompt_text = f"""
You are an AI Data Cleaning Agent.

Analyze the dataset below and return ONLY valid JSON.
Do not add explanations, notes, or markdown.
Return strictly JSON.

Dataset:
{batch_text}

Return format:
{{
    "issues_found": [],
    "cleaning_strategy": [],
    "cleaned_data": [
        {{"column1": "value1", "column2": "value2"}}
    ]
}}
"""

        state = CleaningState(input_text=prompt_text, structured_response="")
        response = self.graph.invoke(state)

        # Extract actual AI text from response
        ai_text = ""
        if isinstance(response, CleaningState):
            ai_text = response.structured_response
        elif isinstance(response, dict):
            ai_text = response.get("structured_response", "")
        else:
            ai_text = str(response)

        # Parse JSON safely
        try:
            ai_json = json.loads(ai_text)
        except json.JSONDecodeError:
            try:
                ai_json = ast.literal_eval(ai_text)
            except Exception:
                # Fallback empty structure if parsing fails
                ai_json = {"issues_found": [], "cleaning_strategy": [], "cleaned_data": []}

        return ai_json

    def clean_data(self, df, batch_size=20):
        cleaned_results = []

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]  # use iloc for DataFrame
            cleaned_batch = self._clean_batch(batch)
            cleaned_results.append(cleaned_batch)

        return cleaned_results  # return list of dicts for easier handling in Streamlit