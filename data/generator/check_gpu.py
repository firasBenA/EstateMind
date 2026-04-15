import torch
print(f"✅ PyTorch version: {torch.__version__}")
print(f"✅ CUDA disponible: {torch.cuda.is_available()}")
if torch.cuda.is_available():
    print(f"🎮 GPU: {torch.cuda.get_device_name(0)}")
    print(f"💾 Mémoire: {torch.cuda.get_device_properties(0).total_memory / 1e9:.1f} GB")
else:
    print("💻 Fonctionnement en mode CPU (plus lent mais fonctionnel)")