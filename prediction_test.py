from tensorflow.keras.models import load_model
import joblib
import pandas as pd
import numpy as np

model = load_model("model/stock_prediction.h5")
scaler = joblib.load('model/scaler.sc') 

df = pd.read_csv("data/markets_historical_vnindex_ind_test.csv", parse_dates=["Date"]).set_index(["Date"]).filter(["Price"]).sort_values(by=["Date"])
df["Price"] = df["Price"].str.replace(",", "").astype(float)

# Scaling data
scaled_data = scaler.fit_transform(df.values)

# Reshape test data
X_test = []
y_test = []

for i in range (60,len(scaled_data)):
  X_test.append(scaled_data[i-60:i,])
  y_test.append(scaled_data[i,])

X_test, y_test = np.array(X_test), np.array(y_test)
print(X_test.shape)
X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 1))

prediction = model.predict(X_test)
prediction = scaler.inverse_transform(prediction)
print(prediction)
