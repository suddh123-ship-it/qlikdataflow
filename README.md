# Qlik Flow Copilot (prototype)

A local web app that turns a plain-English request into a draft `.dfw` file based on a Qlik Cloud Analytics data-flow export. The first version supports OpenAI and Claude (Anthropic).

## Run

1. Install Python 3.10 or newer if it is not already installed.
2. Double-click `run.bat` (or run `python app.py` from this directory).
3. Open [http://127.0.0.1:8765](http://127.0.0.1:8765).
4. Choose OpenAI or Claude, enter an API key created by that provider, write the requested flow, review the generated chain, and download the `.dfw`. The bundled sample loads automatically; uploading a different `.dfw` is optional.
5. Import the downloaded file into a test space in Qlik Cloud Analytics and inspect it before running.

The API key is submitted to the selected model provider for that request. The app does not save the key. The local server binds to `127.0.0.1`; do not expose this prototype to a network as-is.

The app chooses a default model automatically: GPT-5.6 Luna for OpenAI and Claude Sonnet 5 for Anthropic. These defaults are configured near the top of `app.py` and can be changed there if your account does not have access to them.

## Current scope

- Uses the updated `examples/myflow1-expanded.dfw` as the bundled source/target template. Upload another `.dfw` only when you want a different source/target configuration.
- Copies the selected template's source, table, target, and connection references into the generated draft. The bundled sample uses the `superstore` table from a PostgreSQL connection and writes a QVD through the `DataFiles` connection in the `Trial` space. The app does not connect to Qlik or read source rows.
- Replaces the template's processor graph with generated steps, based on configurations seen in the updated export.
- Supports replace empty values, split text into parts, date formatting, uppercase string conversion, numeric addition (field or constant), a forked inner self-join, and sum aggregation.
- Supports one source, one target, and up to 12 requested steps. A join adds a Fork node and joins two branches from that one source; this is not a join between separate tables.
- Sends the plain-English request and selected field names to the LLM. It does not send source rows or credentials from the Qlik connection.
- Does not connect to Qlik Cloud or import/run the result automatically.

The sample only demonstrates uppercase for strings and addition for math. Other Qlik string/math functions and concatenation are not generated yet because their serialized processor configuration is not present in the supplied export.

## Format limitation

Qlik documents `.dfw` export and import between tenants, and the Apps API recognizes `DATAFLOW_PREP` as an app usage type. Qlik's public documentation does not specify the serialized authoring schema for a data-flow graph. This prototype derives its graph structure and processor settings from the attached sample export, so generated files must be validated in a test space with your tenant and Qlik version. The app intentionally rejects templates with other processor types instead of silently dropping them.

To add another processor safely, export a small Qlik flow containing that processor, then extend the serializer and validator using the actual exported configuration.

## Files

- `app.py` — local HTTP server, provider calls, plan validation, and `.dfw` serialization.
- `static/index.html` — standalone browser UI.
- `examples/myflow1-expanded.dfw` — the updated sample supplied for this task.
- `examples/myflow1.dfw` — the earlier sample export.
