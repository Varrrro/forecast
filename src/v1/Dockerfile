FROM python:3.8.2-buster

# Set working directory
WORKDIR /app

# Install dependencies
RUN python3 -m pip install --no-cache-dir --upgrade pip && \
    python3 -m pip install --no-cache-dir \
    gunicorn \
    hug \
    pmdarima

# Copy source files
COPY arima.py .
COPY arima_rest.py .

# Set volume for the models
ENV PKL_PATH="/models"
RUN mkdir /models
VOLUME [ "/models" ]

# Set listening port
ENV PORT=8080
EXPOSE 8080

# Run web server
ENTRYPOINT [ "gunicorn", "-b", "0.0.0.0:8080", "arima_rest:__hug_wsgi__" ]
