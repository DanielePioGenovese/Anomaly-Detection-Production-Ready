FROM vllm/vllm-openai:latest

ENV HF_HOME=/root/.cache/huggingface
EXPOSE 8000

CMD ["--model", "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--gpu-memory-utilization", "0.85", \
     "--max-model-len", "4096", \
     "--max-num-seqs", "8", \
     "--enforce-eager", \
     "--dtype", "bfloat16", \
     "--enable-auto-tool-choice", \
     "--tool-call-parser", "hermes"]