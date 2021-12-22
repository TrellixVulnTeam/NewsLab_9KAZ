from pytorch_pretrained_bert.modeling import BertForSequenceClassification
from pytorch_pretrained_bert.tokenization import BertTokenizer
from flask import request, Flask, render_template
from gevent.pywsgi import WSGIServer
from datetime import datetime
from finbert_utils import *
import pandas as pd
import json
import os

###################################################################################################

DIR = os.path.realpath(os.path.dirname(__file__))
CHUNK_SIZE = 25

model = BertForSequenceClassification.from_pretrained(f"{DIR}/sentiment_model",
													  num_labels=3,
													  cache_dir=None)
tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")
app = Flask(__name__)

###################################################################################################

def predict(sentences):

	"""
	
	Not my code.
	See https://github.com/ProsusAI/finBERT/blob/fcec6c5db7604606ae3ca1cb0db5f60bf8546cbb/predict.py for reference

	Predict sentiments of sentences in a given text. The function first tokenizes sentences, make predictions and write
	results.
	Parameters
	----------
	text: string
		text to be analyzed
	model: BertForSequenceClassification
		path to the classifier model
	write_to_csv (optional): bool
	path (optional): string
		path to write the string
	"""
	
	model.eval()

	label_list = ['positive', 'negative', 'neutral']
	label_dict = {0: 'positive', 1: 'negative', 2: 'neutral'}
	result = pd.DataFrame(columns=['sentence','logit','prediction','sentiment_score'])
	for i, batch in enumerate(chunks(sentences, CHUNK_SIZE)):

		print(f"{datetime.now().isoformat()} - Progress:", i * CHUNK_SIZE / len(sentences))

		examples = [InputExample(str(i), sentence) for i, sentence in enumerate(batch)]

		features = convert_examples_to_features(examples, label_list, 64, tokenizer)

		all_input_ids = torch.tensor([f.input_ids for f in features], dtype=torch.long)
		all_input_mask = torch.tensor([f.input_mask for f in features], dtype=torch.long)
		all_segment_ids = torch.tensor([f.segment_ids for f in features], dtype=torch.long)

		with torch.no_grad():
			logits = model(all_input_ids, all_segment_ids, all_input_mask)
			logits = softmax(np.array(logits))
			sentiment_score = pd.Series(logits[:,0] - logits[:,1])
			predictions = np.squeeze(np.argmax(logits, axis=1))

			batch_result = {'sentence': batch,
							'logit': list(logits),
							'prediction': predictions,
							'sentiment_score':sentiment_score}
			
			batch_result = pd.DataFrame(batch_result)
			result = pd.concat([result,batch_result], ignore_index=True)

	result['prediction'] = result.prediction.apply(lambda x: label_dict[x])

	return result

###################################################################################################

@app.after_request
def after_request(response):
	response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, public, max-age=0"
	response.headers["Expires"] = 0
	response.headers["Pragma"] = "no-cache"
	return response

@app.route("/", methods=["GET", "POST"])
@app.route("/predict", methods=["GET", "POST"])
def get_predictions():

	data = request.get_json()
	sentences = data.get("sentences", None)
	if not sentences:
		return json.dumps({})

	return predict(sentences).T.to_json()

if __name__ == '__main__':

	try:
		http_server = WSGIServer(('', 9602), app)
		http_server.serve_forever()
	except Exception as e:
		print(e)
