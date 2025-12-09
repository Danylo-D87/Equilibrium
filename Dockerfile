# 1. BASE: Download official lightweight Python 3.11 image
FROM python:3.11-slim

# 2. SETTINGS: Prevent .pyc files and buffer output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. WORKSPACE: Create /app directory inside container
WORKDIR /app

# 4. DEPENDENCIES:
# First copy ONLY requirements file
COPY requirements.txt .
# Install dependencies (cached layer)
RUN pip install --no-cache-dir -r requirements.txt

# 5. CODE: Copy entire project into container
COPY . .

# 6. START: Command to execute on container launch
# Run uvicorn, listen on port 8000 on all interfaces (0.0.0.0)
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]