FROM ollama/ollama:latest

# Copy an entrypoint script to pull the Mistral model
COPY ollama_llm/entrypoint.sh /entrypoint.sh

# Make the script executable
RUN chmod +x /entrypoint.sh

# Set the entrypoint to run the script
ENTRYPOINT ["/entrypoint.sh"]