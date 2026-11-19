package binance

import "testing"

func TestParseTrade(t *testing.T) {
	msg := `{"stream":"btcusdt@trade","data":{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"97123.45","q":"0.002","T":1763500000000,"m":true,"M":true}}`
	tr, err := ParseTrade([]byte(msg))
	if err != nil || tr.Symbol != "BTCUSDT" || tr.Price != 97123.45 || tr.Qty != 0.002 || tr.Time.UnixMilli() != 1763500000000 {
		t.Fatalf("got %+v err %v", tr, err)
	}
}
