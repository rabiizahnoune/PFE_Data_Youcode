#!/bin/bash
# Pull the Mistral model
ollama pull mistral

# Start the Ollama server
exec ollama serve