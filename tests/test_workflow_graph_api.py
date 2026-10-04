async def test_workflow_graph_read_is_persistent_and_does_not_start_second_producer(api):
    graph = {
        "nodes": [
            {"id": "start", "kind": "constant", "config": {"value": 3}},
            {"id": "finish", "kind": "output"},
        ],
        "edges": [{"source": "start", "target": "finish"}],
        "output_node": "finish",
    }
    request = {"idempotency_key": "graph-read", "graph": graph, "input_value": None}
    created = await api.post("/v1/workflows/runs", json=request)
    assert created.status_code == 202, created.text
    run_id = created.json()["run_id"]
    first = await api.get(f"/v1/workflows/runs/{run_id}/graph")
    second = await api.get(f"/v1/workflows/runs/{run_id}/graph")
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json() and first.json()["nodes"][0]["config"]["value"] == 3
    events = await api.get(f"/v1/workflows/runs/{run_id}/events")
    assert events.status_code == 200 and "event: end" in events.text
    listed = (await api.get("/v1/workflows/runs")).json()
    assert len([item for item in listed if item["run_id"] == run_id]) == 1
    retry = await api.post("/v1/workflows/runs", json=request)
    assert retry.status_code == 202 and retry.json()["run_id"] == run_id


async def test_workflow_graph_endpoint_rejects_unknown_run_and_does_not_accept_mutation(api):
    missing = await api.get("/v1/workflows/runs/missing/graph")
    assert missing.status_code == 404
    response = await api.post("/v1/workflows/runs/missing/graph", json={})
    assert response.status_code in {405, 404}
