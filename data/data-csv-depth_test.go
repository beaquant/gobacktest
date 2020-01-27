package data

import "testing"

func TestDepthEventFromCSVeData_Load(t *testing.T) {
	symbols := []string{"depth_binance.com_BTC_USDT_2020-01-21"}
	data := &DepthEventFromCSVeData{FileDir: "../examples/testdata/test/"}
	t.Log(data.Load(symbols))

}
