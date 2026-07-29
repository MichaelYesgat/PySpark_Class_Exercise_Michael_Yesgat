# FastAPI Event Project

This project has:

- An **order event** button
- A **navigation event** button
- JavaScript that sends a POST request when either button is clicked
- FastAPI routes that print the received event to the terminal

## Project files

```text
fastapi_event_project/
├── main.py
├── index.html
├── requirements.txt
├── README.md
└── style.css
```

## Run the project

Open a terminal inside the `fastapi_event_project` folder.

### 1. Install the packages

```bash
pip install -r requirements.txt
```

### 2. Start FastAPI

```bash
uvicorn main:app --reload
```

### 3. Open the website

Visit:

```text
http://127.0.0.1:8000
```

Click either button. The page displays the backend response, and the FastAPI
terminal prints one of these messages:

```text
Order event received
Navigation event received
```

## Request flow

```text
Button click
    ↓
JavaScript sends POST request
    ↓
FastAPI receives request
    ↓
FastAPI prints event message
    ↓
Browser displays response message
```

FastAPI's automatic API documentation is also available at:

```text
http://127.0.0.1:8000/docs
```
