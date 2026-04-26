package daa_test

import (
	"context"
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/penny-vault/defensive-asset-allocation/daa"
	"github.com/penny-vault/pvbt/asset"
	"github.com/penny-vault/pvbt/data"
	"github.com/penny-vault/pvbt/engine"
	"github.com/penny-vault/pvbt/portfolio"
)

var _ = Describe("DefensiveAssetAllocation", func() {
	var (
		ctx       context.Context
		snap      *data.SnapshotProvider
		nyc       *time.Location
		startDate time.Time
		endDate   time.Time
	)

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		nyc, err = time.LoadLocation("America/New_York")
		Expect(err).NotTo(HaveOccurred())

		snap, err = data.NewSnapshotProvider("testdata/snapshot.db")
		Expect(err).NotTo(HaveOccurred())

		startDate = time.Date(2024, 11, 1, 0, 0, 0, 0, nyc)
		endDate = time.Date(2025, 12, 31, 0, 0, 0, 0, nyc)
	})

	AfterEach(func() {
		if snap != nil {
			snap.Close()
		}
	})

	runBacktest := func() portfolio.Portfolio {
		strategy := &daa.DefensiveAssetAllocation{}
		acct := portfolio.New(
			portfolio.WithCash(100000, startDate),
			portfolio.WithAllMetrics(),
		)

		eng := engine.New(strategy,
			engine.WithDataProvider(snap),
			engine.WithAssetProvider(snap),
			engine.WithAccount(acct),
		)

		result, err := eng.Backtest(ctx, startDate, endDate)
		Expect(err).NotTo(HaveOccurred())
		return result
	}

	// holdingsAfterDate returns the sorted list of tickers held after
	// processing all transactions up to and including the given date.
	// It accumulates buy/sell quantities to derive the current positions.
	holdingsAfterDate := func(txns []portfolio.Transaction, date string) []string {
		positions := map[string]float64{}
		for _, t := range txns {
			d := t.Date.In(nyc).Format("2006-01-02")
			if d > date {
				break
			}

			switch t.Type {
			case asset.BuyTransaction:
				positions[t.Asset.Ticker] += t.Qty
			case asset.SellTransaction:
				positions[t.Asset.Ticker] -= t.Qty
				if positions[t.Asset.Ticker] < 0.01 {
					delete(positions, t.Asset.Ticker)
				}
			}
		}

		tickers := make([]string, 0, len(positions))
		for ticker := range positions {
			tickers = append(tickers, ticker)
		}
		sort.Strings(tickers)
		return tickers
	}

	It("matches Allocate Smartly allocations for 2025", func() {
		result := runBacktest()
		txns := result.Transactions()

		// Expected holdings from Allocate Smartly, mapped to the paper's
		// original tickers (VWO not IEMG, GSG not PDBC).
		expected := map[string][]string{
			"2024-11-29": {"GLD", "HYG", "IWM", "QQQ", "SPY", "VNQ"},
			"2024-12-31": {"SHY"},                                     // B=2, 100% cash
			"2025-01-31": {"GLD", "GSG", "IWM", "QQQ", "SPY", "VGK"}, // B=0, back to risk
			"2025-03-31": {"EWJ", "GLD", "GSG", "LQD", "VGK", "VWO"},
			"2025-04-30": {"EWJ", "GLD", "HYG", "LQD", "VGK", "VWO"},
			"2025-05-30": {"GLD", "QQQ", "SHY", "VGK"},               // B=1, 50% cash
			"2025-06-30": {"GLD", "IWM", "QQQ", "SPY", "VGK", "VWO"},
			"2025-07-31": {"GLD", "GSG", "IWM", "QQQ", "SPY", "VWO"},
			"2025-08-29": {"EWJ", "GLD", "IWM", "SPY", "VGK", "VWO"},
			"2025-09-30": {"EWJ", "GLD", "IWM", "QQQ", "SPY", "VWO"},
			"2025-10-31": {"EWJ", "GLD", "IWM", "QQQ", "SPY", "VWO"},
			"2025-11-28": {"EWJ", "GLD", "IWM", "QQQ", "SPY", "VGK"},
		}

		for date, exp := range expected {
			actual := holdingsAfterDate(txns, date)
			Expect(actual).To(Equal(exp), "holdings mismatch on %s", date)
		}
	})

	It("shifts to 50%% cash in May 2025 when one canary asset signals", func() {
		result := runBacktest()
		txns := result.Transactions()

		mayHoldings := holdingsAfterDate(txns, "2025-05-30")

		// B=1 means 50% cash. Should hold 3 risk assets + 1 cash asset.
		Expect(mayHoldings).To(HaveLen(4))

		cashAssets := map[string]bool{"SHY": true, "IEF": true, "LQD": true}
		hasCash := false
		for _, t := range mayHoldings {
			if cashAssets[t] {
				hasCash = true
				break
			}
		}
		Expect(hasCash).To(BeTrue(), "expected a cash universe asset in May allocation")
	})
})
