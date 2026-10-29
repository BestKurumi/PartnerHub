//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"partner-hub/internal/delivery"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
)

func TestManualQueryHistory(t *testing.T) {
	db := manualDB(t)
	id := manualTarget(t, db, delivery.RetryDelivery)
	var msg string
	if err := db.QueryRow("SELECT msg_id FROM delivery_task WHERE delivery_id=?", id).Scan(&msg); err != nil {
		t.Fatal(err)
	}
	for i := range 22 {
		r := delivery.RetryRequest{OperationID: model.NewID(), Kind: delivery.RetryDelivery, TargetID: id, Operator: "运维", Reason: fmt.Sprintf("已修复配置-%d", i)}
		if _, err := hubmysql.NewManualStore(db).ExecuteRetry(context.Background(), r); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("UPDATE delivery_task SET status='NEEDS_ATTENTION' WHERE delivery_id=?", id); err != nil {
			t.Fatal(err)
		}
	}
	w := queryGet(db, "/api/v1/messages/"+msg)
	var result struct {
		ManualOperations []struct {
			OperationID, Kind, TargetID, Operator, Reason, State string
			RetryCycle                                           int
		}
		ManualOperationsTruncated bool
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || len(result.ManualOperations) != 20 || !result.ManualOperationsTruncated {
		t.Fatalf("manual history absent or unbounded: HTTP%d %s", w.Code, w.Body.String())
	}
	for _, r := range result.ManualOperations {
		if r.OperationID == "" || r.Kind != delivery.RetryDelivery || r.TargetID != id || r.Operator != "运维" || r.State != "PENDING" || r.RetryCycle < 2 {
			t.Fatalf("not the saved command result: %+v", r)
		}
	}
	other := manualTarget(t, db, delivery.RetryRouting)
	r := delivery.RetryRequest{OperationID: model.NewID(), Kind: delivery.RetryRouting, TargetID: other, Operator: "运维", Reason: "修复绑定查询"}
	if _, err := hubmysql.NewManualStore(db).ExecuteRetry(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	w = queryGet(db, "/api/v1/messages/"+other)
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || len(result.ManualOperations) != 1 || result.ManualOperations[0].OperationID != r.OperationID || result.ManualOperationsTruncated {
		t.Fatalf("route operation query: HTTP%d %s", w.Code, w.Body.String())
	}
}
