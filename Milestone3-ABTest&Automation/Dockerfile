FROM python:3.8-bookworm

WORKDIR /app

COPY . .

RUN mv flaskr/data/model/svd_model.joblib flaskr/data/model/model.joblib

RUN pip install .

CMD ["waitress-serve", "--port=8083", "--threads=100", "--call", "flaskr:create_app"]

EXPOSE 8083