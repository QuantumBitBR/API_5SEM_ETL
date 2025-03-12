# API_5SEM_ETL

## Requirements

Before running the project, make sure you have the following installed:

- **Python** (recommended Python 3.7 or higher)
- **pip** (Python's package manager)

## Installation

Follow the steps below to set up the environment and install the necessary dependencies for the project.

1. **Clone the repository** to your local machine:

    ```bash
    git clone https://github.com/QuantumBitBR/API_5SEM_ETL.git
    cd API_5SEM_ETL
    ```

2. **Environment Variables**

   Create a `.env` file in the root directory of your project and add the following environment variables:

   ```bash
    TAIGA_API_URL=https://api.taiga.io/api/v1
    TAIGA_USERNAME=<username>
    TAIGA_PASSWORD=<password>
    DATABASE_HOST=localhost
    DATABASE_PORT=5432
    DATABASE_NAME=dbo
    DATABASE_USER=taiga
    DATABASE_PASSWORD=taiga
   ```

3. **Create a virtual environment** for the project (recommended):

   For Windows:

    ```bash
    python -m venv venv
    venv\Scripts\activate
    ```

    For Linux/MacOS:

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

4. **Install the dependencies** for the project:

    The project uses a `requirements.txt` file to list the necessary libraries.

    ```bash
    pip install -r requirements.txt
    ```

## Running the Project

Once the dependencies are installed, you can run the project using the following command:

```bash
python3 main.py
```
