# tap-gpt

`tap-gpt` is a Singer tap for gpt.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Configuration

A full list of supported settings and capabilities for this tap is available by running:

```bash
tap-gpt --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can easily run `tap-gpt` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-gpt --version
tap-gpt --help
tap-gpt --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

```bash
poetry run pytest
```

### Testing with Meltano

```bash
pipx install meltano
cd tap-gpt
meltano install
meltano invoke tap-gpt --version
meltano run tap-gpt target-jsonl
```

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions.
# tap-gpt
