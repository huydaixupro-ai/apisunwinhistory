from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import time

# Cấu hình môi trường TensorFlow
os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
os.environ["CUDA_VISIBLE_DEVICES"] = ""

app = Flask(__name__)
CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

# Cấu hình chung
max_length = 15

def encode_base64x(base64, img_width, img_height):
    img = tf.io.decode_base64(base64)
    img = tf.io.decode_png(img, channels=1)
    img = tf.image.convert_image_dtype(img, tf.float32)
    img = tf.image.resize(img, [img_height, img_width])
    img = tf.transpose(img, perm=[1, 0, 2])
    return {"image": img}

def decode_batch_predictions(pred, num_to_char):
    input_len = np.ones(pred.shape[0]) * pred.shape[1]
    results = keras.backend.ctc_decode(pred, input_length=input_len, greedy=True)[0][0][:, :max_length]
    output_text = [tf.strings.reduce_join(num_to_char(res)).numpy().decode("utf-8") for res in results]
    return output_text

# Model BIDV
characters_bidv = ['2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'x', 'y', 'z']
char_to_num_bidv = layers.StringLookup(vocabulary=list(characters_bidv), mask_token=None)
num_to_char_bidv = layers.StringLookup(vocabulary=char_to_num_bidv.get_vocabulary(), mask_token=None, invert=True)
class CTCLayer(layers.Layer):
    def __init__(self, name=None):
        super().__init__(name=name)
        self.loss_fn = keras.backend.ctc_batch_cost
model_bidv = keras.models.load_model("model_bidv.h5", custom_objects={"CTCLayer": CTCLayer})
prediction_model_bidv = keras.models.Model(model_bidv.get_layer(name="image").input, model_bidv.get_layer(name="dense2").output)

# Model MBBank
characters_mb = ['K', 'M', 'C', 'e', 'g', 'k', 'u', 'z', 't', '3', 'U', 'a', '5', 'A', 'y', 'H', 'q', 'Z', 'V', '7', 'Q', '2', '4', 'Y', '-', 'h', '8', 'v', '6', 'd', 'b', 'n', 'p', 'P', 'E', 'c', 'm', 'D', 'B', '9', 'N', 'G']
char_to_num_mb = layers.StringLookup(vocabulary=list(characters_mb), mask_token=None)
num_to_char_mb = layers.StringLookup(vocabulary=char_to_num_mb.get_vocabulary(), mask_token=None, invert=True)
json_file_mb = open('model_mb.json', 'r')
loaded_model_json = json_file_mb.read()
json_file_mb.close()
model_mb = keras.models.model_from_json(loaded_model_json)
model_mb.load_weights("model_mb.h5")

# Model VCB
characters_vcb = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
char_to_num_vcb = layers.StringLookup(vocabulary=list(characters_vcb), mask_token=None)
num_to_char_vcb = layers.StringLookup(vocabulary=char_to_num_vcb.get_vocabulary(), mask_token=None, invert=True)
model_vcb = keras.models.load_model("vcb_model.h5", custom_objects={"CTCLayer": CTCLayer})
prediction_model_vcb = keras.models.Model(model_vcb.get_layer(name="image").input, model_vcb.get_layer(name="dense2").output)

# API endpoints
@app.route("/api/captcha/bidv", methods=["POST"])
def captcha_bidv():
    content = request.json
    imgstring = content['base64']
    image_encode = encode_base64x(imgstring.replace("+", "-").replace("/", "_"), 145, 50)["image"]
    preds = prediction_model_bidv.predict(np.array([image_encode]))
    captcha = decode_batch_predictions(preds, num_to_char_bidv)[0].replace('[UNK]', '').replace('-', '')
    return jsonify(status="success", captcha=captcha)

@app.route("/api/captcha/mbbank", methods=["POST"])
def captcha_mb():
    content = request.json
    imgstring = content['base64']
    image_encode = encode_base64x(imgstring.replace("+", "-").replace("/", "_"), 320, 80)["image"]
    preds = model_mb.predict(np.array([image_encode]))
    captcha = decode_batch_predictions(preds, num_to_char_mb)[0].replace('[UNK]', '').replace('-', '')
    return jsonify(status="success", captcha=captcha)

@app.route("/api/captcha/vcb", methods=["POST"])
def captcha_vcb():
    content = request.json
    imgstring = content['base64']
    image_encode = encode_base64x(imgstring.replace("+", "-").replace("/", "_"), 155, 50)["image"]
    preds = prediction_model_vcb.predict(np.array([image_encode]))
    captcha = decode_batch_predictions(preds, num_to_char_vcb)[0].replace('[UNK]', '').replace('-', '')
    return jsonify(status="success", captcha=captcha)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
