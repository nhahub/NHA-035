import streamlit as st
import re
from transformers import AutoTokenizer,BertForSequenceClassification,pipeline
import torch
@st.cache_resource
def load_model():
    model_path = "sentiment_model"
   
    tokenizer_type = AutoTokenizer.from_pretrained(model_path)
    
   
    model = BertForSequenceClassification.from_pretrained(
        model_path,device_map=None)
        
    pipeline_model = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer_type)

    return pipeline_model

sentiment_pipeline=load_model()
st.set_page_config(page_title="Arabic Sentiment Analysis",page_icon="💬")
st.title("Sentiment Analysis of Product Reviews")
st.subheader("Sentiment Analysis Using Custom MARBERT Model")
text=st.text_area("Enter an Arabic Sentence😊")
if st.button("Analysis📈"):
    if text.strip():
        if not re.match(r'^[\u0600-\u06FF\s]+$',text):
            st.warning("⚠Please Enter Arabic Sentence only")
        else:
            with st.spinner("Analysis......"):
                result_analysis=sentiment_pipeline(text)[0]
                label=result_analysis['label']
                score_conf=round(result_analysis['score'],3)
                label_map = {
                    "LABEL_0": "سلبي 😞",
                    "LABEL_1": "محايد 😐",
                    "LABEL_2": "أيجابي 😄"
                }
                arabic_label=label_map.get(label,"غير معروف")
                st.success("✅ Analysis completed successfully!")
                st.write(f"Sentiment: {arabic_label}")
                st.write(f"Confidence score: {score_conf}")
    else:
        st.write("Please enter a sentence first.")