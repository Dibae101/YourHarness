# YourHarness as a personal compute pool

The product is for someone who has a laptop in front of them, a more capable machine somewhere else, and occasionally a friend willing to help. They should be able to ask for a result from one screen. YourHarness can choose a machine by what it actually offers, run the work there, and bring the output back. A local model already installed on a friend's desktop is a resource worth lending in its own right.

## Workloads in this version

Start the coordinator from a checkout with `npm start`, sign in to the pool as its owner, and open **Home → Start a workload**. Use the same checkout or app version for the participating computers. Phones and tablets can operate the workspace and contribute supported browser work. The AI workers below need the native agent on a computer.

| What you want | What gets scheduled | Output |
| --- | --- | --- |
| Local LLM | Each prompt goes to a computer offering the selected Ollama model and identical manifest digest. Several prompts can run in parallel. | Ordered JSON Lines with the prompts, full answers, model revision and finish reasons |
| Train a model | Numeric features and target from a CSV or JSON Lines dataset are partitioned across eligible computers. The coordinator combines centered statistics and fits a linear or ridge regression with an intercept. | Portable JSON weights, feature order, training row count and training RMSE. The job details can predict a new value. |
| Training experiments | One user-defined Python training function per parameter combination. Each trial receives the full dataset, verifies its SHA-256 and runs on one owned computer. | Ordered JSON results plus an optional downloadable model checkpoint per trial. |
| Model evaluation | Score an uploaded Python or TorchScript classifier against labels. | Accuracy, per-class metrics and optional predictions |
| Run an agent | Give an installed coding agent a repository and a task. Select eligible computers in the Agents workspace and inspect the resulting changes. | A branch in a local repository or a draft pull request on GitHub |
| Data or Python | Aggregate, join, reshape, or apply your own Python function to records or items. | A summary or downloadable records |
| Review Studio | Render supported independent frames, then assemble them. | A playable MP4 |

## Use a local LLM

1. On the computer with enough memory, start Ollama and install a local text model of your choice. Use its app or `ollama pull <model>` on that machine. The YourHarness agent contacts Ollama on loopback port 11434. Set `HARNESS_OLLAMA_PORT` on the agent if you chose a different local port. For a WSL agent, that endpoint must be reachable within WSL. [Ollama model listing](https://docs.ollama.com/api/tags) · [Model capabilities](https://docs.ollama.com/api-reference/show-model-details)
2. In **Home → Local LLM**, choose the model revision. The selector lists the computers offering it. Enter one prompt, or separate several requests with blank lines. **Check where it would run** explains which computers qualify. Each request is an independent completion. [Ollama generation API](https://docs.ollama.com/api/generate)
3. For a friend's computer, issue a friend code and have them select **Lend my installed Ollama text models** in desktop pairing, or start their native agent with `--share-models`. They can quit the agent to stop lending, or restart the CLI with `--no-share-models`. The submitting user must also enable **Allow friends** for the prompts. The lending computer can read everything in the prompt. Both parties need network access to the coordinator. An internet helper can use an existing private VPN or a reachable HTTPS pool address. [Device setup](devices.md)
4. Open the finished job to read its preview or download all responses. Models are installed by each device's owner. One model must fit on the computer serving the prompt.

The same flow is available in Python. The model name below should come from your pool's inventory.

```python
from yourharness import jobs

for model in jobs.local_models():
    print(model["name"], model["digest"], model["devices"])

answers = jobs.generate("my-model:latest", ["Summarize these notes: …", "Suggest a title for: …"])
print(answers[0]["response"])
```

## Bring a training function

Add a CSV or JSON Lines file in the **Datasets** tab. For a numeric target, **Train a model** fits up to 32 distinct feature columns. A positive ridge penalty shrinks the weights. The downloaded model predicts an output as the intercept plus the sum of each feature multiplied by its coefficient. Training RMSE measures fit on the training data. Use separate examples to measure generalization.

For an experiment with another algorithm, open **Training experiments**. Set the parameter grid, edit the function and select its Python environment. `ctx["datasetPath"]` points to the full input for this attempt. `ctx["datasetFormat"]` and `ctx["datasetSha256"]` identify it. The starting function compares polynomial degrees on a reproducible holdout. You can replace it with a classifier, neural network or fine-tuning loop that fits on the selected host. Return JSON-serializable metrics. To retain a trained model, write a single file of up to 4 GiB to `ctx["checkpointPath"]`. Each accepted trial uploads that file. Job details link to each checkpoint and the full result includes its SHA-256. Other working files are temporary.

Here is a neural-network training function for a CSV with columns named `x` and `y`. Use it in the trial editor with the grid `{"width":[8,16,32]}`, or in a Python file with the SDK call below. The example uses a managed Python environment with NumPy and scikit-learn.

```python
def train(params, ctx):
    import csv
    import joblib
    import numpy as np
    from sklearn.neural_network import MLPRegressor
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    with open(ctx["datasetPath"], encoding="utf-8-sig", newline="") as source:
        rows = list(csv.DictReader(source))
    x = np.array([[float(row["x"])] for row in rows])
    y = np.array([float(row["y"]) for row in rows])
    train_x, valid_x, train_y, valid_y = train_test_split(x, y, random_state=42)
    model = make_pipeline(StandardScaler(), MLPRegressor(
        hidden_layer_sizes=(params["width"],), random_state=42, max_iter=500
    ))
    model.fit(train_x, train_y)
    joblib.dump(model, ctx["checkpointPath"])
    error = model.predict(valid_x) - valid_y
    return {"width": params["width"], "validationRMSE": float(np.sqrt(np.mean(error ** 2)))}
```

```python
from yourharness import jobs

data = jobs.dataset("observations.csv")
scores = jobs.train_trials(train, data, {"width": [8, 16, 32]}, python="3.12",
                          requirements=["numpy", "scikit-learn"], mode="distributed",
                          approve_setup=True, downloads=True, manifest=True)
print(scores["checkpoints"])
```

Native and managed trials can require detected NVIDIA CUDA or Apple Metal hardware and a minimum amount of free system memory. The training code and installed framework must actually use the accelerator. Each trial needs room for a local copy. Native and managed workers can reuse datasets in their bounded artifact cache. Containers receive a staged copy per attempt. Custom training code runs on owned computers. A friend's native agent can instead help fit built-in regression on a dataset explicitly marked **shareable**, or participate in agent tasks with the existing lender permissions. [Agent tasks](agent-tasks.md)

## Proof of concept to run later

| Exercise | Evidence to collect |
| --- | --- |
| A laptop uses a larger local model on another computer | Put the chosen model only on the helper. Confirm the plan names it, submit two prompts, and match the downloaded revision to its inventory. With two hosts offering the same model, use distributed mode and compare the per-device contributions and elapsed time. |
| A friend lends a model | Join with a friend code and keep model lending off. Then opt in on the lender. Compare the requester's inventory before and after enabling friend processing, and verify which computer handled the prompts. |
| Regression over shards | Generate enough rows for `y = 3 + 2*x` to produce several partitions. Choose distributed mode. Expect an intercept near 3, a coefficient near 2 and a prediction near 23 for `x = 10`. Record which computers contributed and the time spent transferring data. |
| Custom model training | Try three widths on the same held-out data. Expect three trial results, a shared dataset digest in the run provenance, validation errors, worker names and three downloadable checkpoints. Require a GPU absent from the pool and check that the planner identifies the missing hardware. |
| Agent task | Use a local Git repository, assign an installed coding agent a small documentation change, and inspect the resulting branch and diff. |
| Interruption | During a multi-partition distributed run, disconnect a helper. Check which completed partitions survive, where unfinished work goes, and what the user sees if no compatible worker remains. |

The commercial hypothesis is that small teams will pay for deciding **which device can do this work, what the other person must share, and whether the transfer is worth it**. Record end-to-end time, setup minutes on each side, output quality and whether the user would repeat the workflow. Compare with running on the powerful machine directly. The benefit should be demonstrated separately for each workload.

## Product ideas for later revision

- Generalize Review Studio's one-job invitations to local inference and dataset jobs. Let a lender choose a single job, time window, model, and budget. Current native model consent lasts for the pool membership until changed.
- Add model-aware memory estimates, measured prompt throughput by model and quantization, and disk and bandwidth estimates before starting an experiment.
- Add mid-training resume, experiment comparisons and a model registry so the winning custom trial can go directly into inference.
- Allow bounded, multi-step agents to use a shared local model. The existing coding agents use their own installed CLIs and authentication.
- Evaluate tensor-parallel inference and synchronized multi-host neural training on suitable networks. The current system partitions independent work. A running laptop application still has its own RAM, and a model that exceeds every individual host's available memory needs a different backend.
- Make persistent storage lending a separate opt-in with quotas, encrypted replicas and recovery after a lender disconnects. Today's coordinator stores datasets and results.
