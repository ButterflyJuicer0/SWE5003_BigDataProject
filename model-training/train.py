# simplifyweibo_4_moods
import torch
import numpy as np
import pandas as pd
from torch import nn
from torch.optim import Adam
from tqdm import tqdm
from transformers import AutoModel,AutoTokenizer
import matplotlib.pyplot as plt
path = 'model'
mode_name = 'bert-base-chinese'
tokenizer = AutoTokenizer.from_pretrained(path)
model_original = AutoModel.from_pretrained(path,num_labels=15)

df = pd.read_csv('processed_data.csv')
np.random.seed(112)
df_train, df_val, df_test = np.split(df.sample(frac=1, random_state=42),
                                     [int(.8 * len(df)), int(.9 * len(df))])  # 拆分为训练集、验证集和测试集，比例为 80:10:10。


class Dataset(torch.utils.data.Dataset):
    def __init__(self, df):
        self.labels = np.array(df['label'])
        self.texts = [tokenizer(text,
                                padding='max_length',
                                max_length=16,
                                truncation=True,
                                return_tensors="pt")
                      for text in df['review']]

    def classes(self):
        return self.labels

    def __len__(self):
        return len(self.labels)

    def get_batch_labels(self, idx):
        # Fetch a batch of labels
        return np.array(self.labels[idx])

    def get_batch_texts(self, idx):
        # Fetch a batch of inputs
        return self.texts[idx]

    def __getitem__(self, idx):
        batch_texts = self.get_batch_texts(idx)
        batch_y = self.get_batch_labels(idx)
        return batch_texts, batch_y



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

def plot_metrics(train_losses, val_losses, train_accuracies, val_accuracies):

    plt.clf()

    plt.figure(figsize=(12, 6))

    # Loss
    plt.subplot(1, 2, 1)
    plt.plot(train_losses, label='Train Loss')
    plt.plot(val_losses, label='Validation Loss')
    plt.title('Loss over epochs')
    plt.xlabel('Epochs')
    plt.ylabel('Loss')
    plt.legend()

    # ACC
    plt.subplot(1, 2, 2)
    plt.plot(train_accuracies, label='Train Accuracy')
    plt.plot(val_accuracies, label='Validation Accuracy')
    plt.title('Accuracy over epochs')
    plt.xlabel('Epochs')
    plt.ylabel('Accuracy')
    plt.legend()

    plt.pause(0.1)  # pause a bit so that plots are updated
    plt.show(block=False)

def train(model, train_data, val_data, learning_rate, epochs, batch_size):
    train_losses, val_losses = [], []
    train_accuracies, val_accuracies = [], []



    train, val = Dataset(train_data), Dataset(val_data)

    train_dataloader = torch.utils.data.DataLoader(train, batch_size, shuffle=True)
    val_dataloader = torch.utils.data.DataLoader(val, batch_size)

    use_cuda = torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")

    criterion = nn.CrossEntropyLoss()
    optimizer = Adam(model.parameters(), lr=learning_rate)

    if use_cuda:
        model = model.cuda()
        criterion = criterion.cuda()
    # Learning rate scheduler
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=1, gamma=0.1)

    # 开始进入训练循环
    for epoch_num in range(epochs):

        total_acc_train = 0
        total_loss_train = 0


        for train_input, train_label in tqdm(train_dataloader):
            train_label = train_label.to(device)
            mask = train_input['attention_mask'].to(device)
            input_id = train_input['input_ids'].squeeze(1).to(device)

            output = model(input_id, mask)

            batch_loss = criterion(output, train_label.long())
            total_loss_train += batch_loss.item()

            acc = (output.argmax(dim=1) == train_label).sum().item()
            total_acc_train += acc

            model.zero_grad()
            batch_loss.backward()
            optimizer.step()

        scheduler.step()

        updated_lr = optimizer.param_groups[0]['lr']

        total_acc_val = 0
        total_loss_val = 0

        avg_train_loss = total_loss_train / len(train_data)
        avg_train_acc = total_acc_train / len(train_data)
        train_losses.append(avg_train_loss)
        train_accuracies.append(avg_train_acc)

        with torch.no_grad():

            for val_input, val_label in val_dataloader:

                val_label = val_label.to(device)
                mask = val_input['attention_mask'].to(device)
                input_id = val_input['input_ids'].squeeze(1).to(device)
                output = model(input_id, mask)
                batch_loss = criterion(output, val_label.long())
                total_loss_val += batch_loss.item()
                acc = (output.argmax(dim=1) == val_label).sum().item()
                total_acc_val += acc


        print(
            f'''Epochs: {epoch_num + 1} 
              | Train Loss: {total_loss_train / len(train_data): .3f} 
              | Train Accuracy: {total_acc_train / len(train_data): .3f} 
              | Val Loss: {total_loss_val / len(val_data): .3f} 
              | Val Accuracy: {total_acc_val / len(val_data): .3f} 
              | Updated LR: {updated_lr}''')
        avg_val_loss = total_loss_val / len(val_data)
        avg_val_acc = total_acc_val / len(val_data)
        val_losses.append(avg_val_loss)
        val_accuracies.append(avg_val_acc)
        plot_metrics(train_losses, val_losses, train_accuracies, val_accuracies)




EPOCHS = 10
model = BertClassifier()
LR = 1e-5
Batch_Size = 128
train(model, df_train, df_val, LR, EPOCHS, Batch_Size)
torch.save(model.state_dict(), 'BERT-weibo.pt')


# Evaluate
def evaluate(model, test_data, batch_size):
    test = Dataset(test_data)
    test_dataloader = torch.utils.data.DataLoader(test, batch_size=batch_size)
    use_cuda = torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")
    if use_cuda:
        model = model.cuda()
    total_acc_test = 0
    with torch.no_grad():
        for test_input, test_label in test_dataloader:
            test_label = test_label.to(device)
            mask = test_input['attention_mask'].to(device)
            input_id = test_input['input_ids'].squeeze(1).to(device)
            output = model(input_id, mask)
            acc = (output.argmax(dim=1) == test_label).sum().item()
            total_acc_test += acc
    print(f'Test Accuracy: {total_acc_test / len(test_data): .3f}')


evaluate(model, df_test, Batch_Size)