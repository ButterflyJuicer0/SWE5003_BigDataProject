import torch
from torch import nn
from transformers import AutoModel,AutoTokenizer

path = 'model'
mode_name = 'bert-base-chinese'
tokenizer = AutoTokenizer.from_pretrained(path)
model_original = AutoModel.from_pretrained(path,num_labels=15)

def get_label_string(label):
    labels = {0:'positive',
              1:'neutral',
              2:'negative'
    }
    for key, value in labels.items():
        if value == label:
            return key
    return None



class BertClassifier(nn.Module):
    def __init__(self, dropout=0.5):
        super(BertClassifier, self).__init__()
        self.bert = model_original
        self.dropout = nn.Dropout(dropout)
        self.linear = nn.Linear(768, 3)
        self.relu = nn.ReLU()

    def forward(self, input_id, mask):
        _, pooled_output = self.bert(input_ids=input_id, attention_mask=mask, return_dict=False)
        dropout_output = self.dropout(pooled_output)
        linear_output = self.linear(dropout_output)
        final_layer = self.relu(linear_output)
        return final_layer


model = BertClassifier()
model.load_state_dict(torch.load('BERT-weibo.pt'))
model.eval()
text = '你好'
text_input = tokenizer(text, padding='max_length', max_length=16, truncation=True, return_tensors="pt")
mask = text_input['attention_mask']
input_id = text_input['input_ids']
output = model(input_id, mask)
output = output.argmax(dim=1)
output = output.item()
label_string =  get_label_string(output)
print(label_string)