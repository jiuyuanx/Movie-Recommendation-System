FROM python:3.8-bookworm

WORKDIR /app

COPY . .

RUN rm -rf flaskr/data/model/recommendations.csv
RUN mv flaskr/data/model/knn_model.joblib flaskr/data/model/model.joblib

RUN mv flaskr/data/model/recommendations_knn.csv flaskr/data/model/recommendations.csv

RUN pip install .

CMD ["waitress-serve", "--port=8084", "--threads=100", "--call", "flaskr:create_app"]

EXPOSE 8084