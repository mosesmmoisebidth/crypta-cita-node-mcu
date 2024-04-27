This is the simple FastAPI code for real time water_level data streaming over the websockets
`Running Command`
`uvicorn crypta:app --host 127.0.0.1 --port 8001 --reload`
`Running the Client code`
`uvicorn client:app --host 127.0.0.1 --post 8001 --reload`

<p>Feel free to change the ports and add more other codes</p>
