from transformers import AutoModel,AutoTokenizer
path = 'model'
mode_name = 'bert-base-chinese'
tokenizer = AutoTokenizer.from_pretrained(path)
model = AutoModel.from_pretrained(path)

