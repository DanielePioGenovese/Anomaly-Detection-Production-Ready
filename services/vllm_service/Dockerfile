FROM vllm/vllm-openai:latest

# Model is pulled from HuggingFace on first start and cached in the volume.
# Set HF_HOME to a mounted volume so it survives container restarts.
ENV HF_HOME=/root/.cache/huggingface

EXPOSE 8000

# --gpu-memory-utilization: tune down if you have less VRAM
# --max-model-len:          reduce if you hit OOM on 8 B
CMD ["--model", "Qwen/Qwen3-8B-Instruct", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--gpu-memory-utilization", "0.90", \
     "--max-model-len", "8192", \
     "--dtype", "bfloat16"]