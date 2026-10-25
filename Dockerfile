FROM python:3.13.7-alpine3.21

WORKDIR /app

RUN pip install pyzmq grpcio-tools

COPY ./mensagem.proto .

RUN python -m grpc_tools.protoc -I. --python_out=. mensagem.proto

CMD ["python", "main.py"]