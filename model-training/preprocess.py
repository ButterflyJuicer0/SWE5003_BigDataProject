import pandas as pd
df=pd.read_csv('simplifyweibo_4_moods.csv')
from transformers import pipeline

distilled_student_sentiment_classifier = pipeline(
    model="lxyuan/distilbert-base-multilingual-cased-sentiments-student",
    return_all_scores=False
)



def tag(text):
    sentiment = distilled_student_sentiment_classifier(text)[0]['label']
    if sentiment =='positive':
        return 0
    elif sentiment == 'neutral':
        return 1
    else:
        return 2
selected_data = df.head(20000)
selected_data['label']=selected_data['review'].apply(tag)


selected_data.to_csv('./processed_data.csv')
