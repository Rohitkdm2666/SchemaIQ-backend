import google.generativeai as genai
import os

# paste your key here temporarily
key = input("Paste your Gemini API key: ").strip()
genai.configure(api_key=key)

print("\nAvailable models that support generateContent:\n")
for m in genai.list_models():
    if "generateContent" in m.supported_generation_methods:
        print(f"  {m.name}")