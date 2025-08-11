FROM python:3.12-slim


# Installer Poetry via pip
RUN pip install poetry

WORKDIR /app

COPY pyproject.toml poetry.lock /app/
RUN poetry install --no-root --without dev

# Copy the source code into the container.
COPY src/ /app/src/
COPY data/ /app/data/

# Expose the port that the application listens on.
EXPOSE 8501

# Run the application.
CMD poetry run python src/init_db.py && poetry run streamlit run src/interface.py   --server.port 8501 --server.address 0.0.0.0