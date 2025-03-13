package api

import (
	"fmt"
	"net/http"
)

func TestAPIHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Testing api handler")
}
