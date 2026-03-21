// Copyright 2021-2026
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	_ "embed"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/penny-vault/pvbt/asset"
	"github.com/penny-vault/pvbt/data"
	"github.com/penny-vault/pvbt/engine"
	"github.com/penny-vault/pvbt/portfolio"
	"github.com/penny-vault/pvbt/universe"
)

//go:embed README.md
var description string

type DefensiveAssetAllocation struct {
	RiskUniverse       universe.Universe `pvbt:"risk-universe" desc:"List of ETF, Mutual Fund, or Stock tickers in the risk universe" default:"SPY,IWM,QQQ,VGK,EWJ,VWO,VNQ,GSG,GLD,TLT,HYG,LQD" suggest:"DAA-G12=SPY,IWM,QQQ,VGK,EWJ,VWO,VNQ,GSG,GLD,TLT,HYG,LQD|DAA-G6=SPY,VEA,VWO,LQD,TLT,HYG|DAA1-G4=SPY,VEA,VWO,AGG|DAA1-G12=SPY,IWM,QQQ,VGK,EWJ,VWO,VNQ,GSG,GLD,TLT,HYG,LQD|DAA1-U1=SPY"`
	ProtectiveUniverse universe.Universe `pvbt:"protective-universe" desc:"Canary universe that signals when to shift to cash" default:"VWO,BND" suggest:"DAA-G12=VWO,BND|DAA-G6=VWO,BND|DAA1-G4=VWO,BND|DAA1-G12=VWO,BND|DAA1-U1=VWO,BND"`
	CashUniverse       universe.Universe `pvbt:"cash-universe" desc:"Safe-haven assets for defensive allocation" default:"SHY,IEF,LQD" suggest:"DAA-G12=SHY,IEF,LQD|DAA-G6=SHY,IEF,LQD|DAA1-G4=SHV,IEF,UST|DAA1-G12=SHV,IEF,UST|DAA1-U1=SHV,IEF,UST"`
	Breadth            int               `pvbt:"breadth" desc:"Breadth parameter determining cash fraction from canary signals" default:"2" suggest:"DAA-G12=2|DAA-G6=2|DAA1-G4=1|DAA1-G12=1|DAA1-U1=1"`
	TopT               int               `pvbt:"top-t" desc:"Number of top risk assets to invest in" default:"6" suggest:"DAA-G12=6|DAA-G6=6|DAA1-G4=4|DAA1-G12=2|DAA1-U1=1"`
}

func (s *DefensiveAssetAllocation) Name() string {
	return "Defensive Asset Allocation"
}

func (s *DefensiveAssetAllocation) Setup(_ *engine.Engine) {}

func (s *DefensiveAssetAllocation) Describe() engine.StrategyDescription {
	return engine.StrategyDescription{
		ShortCode:   "daa",
		Description: description,
		Source:      "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3212862",
		Version:     "1.0.1",
		VersionDate: time.Date(2026, 3, 14, 0, 0, 0, 0, time.UTC),
		Schedule:    "@monthend",
		Benchmark:   "VFINX",
	}
}

func (s *DefensiveAssetAllocation) Compute(ctx context.Context, eng *engine.Engine, strategyPortfolio portfolio.Portfolio, batch *portfolio.Batch) error {
	// Fetch 13-month window so that after monthly downsample we have >= 13 rows
	// for Pct(12) to produce a valid value. Uses adjusted close (total return).
	riskDF, err := s.RiskUniverse.Window(ctx, portfolio.Months(13), data.AdjClose)
	if err != nil {
		return fmt.Errorf("failed to fetch risk universe prices: %w", err)
	}

	protectiveDF, err := s.ProtectiveUniverse.Window(ctx, portfolio.Months(13), data.AdjClose)
	if err != nil {
		return fmt.Errorf("failed to fetch protective universe prices: %w", err)
	}

	cashDF, err := s.CashUniverse.Window(ctx, portfolio.Months(13), data.AdjClose)
	if err != nil {
		return fmt.Errorf("failed to fetch cash universe prices: %w", err)
	}

	// Downsample to monthly frequency (use last value in each month).
	riskMonthly := riskDF.Downsample(data.Monthly).Last()
	protectiveMonthly := protectiveDF.Downsample(data.Monthly).Last()
	cashMonthly := cashDF.Downsample(data.Monthly).Last()

	if riskMonthly.Len() < 13 || protectiveMonthly.Len() < 13 || cashMonthly.Len() < 13 {
		return nil
	}

	// Compute 13612W momentum for each universe.
	riskMom := momentum13612W(riskMonthly).Last()
	canaryMom := momentum13612W(protectiveMonthly).Last()
	cashMom := momentum13612W(cashMonthly).Last()

	if riskMom.Len() == 0 || canaryMom.Len() == 0 || cashMom.Len() == 0 {
		return nil
	}

	// Record momentum scores as annotations.
	for _, a := range riskMom.AssetList() {
		for _, m := range riskMom.MetricList() {
			v := riskMom.Value(a, m)
			if !math.IsNaN(v) {
				batch.Annotate(a.Ticker+"/"+string(m), strconv.FormatFloat(v, 'f', -1, 64))
			}
		}
	}
	for _, a := range canaryMom.AssetList() {
		for _, m := range canaryMom.MetricList() {
			v := canaryMom.Value(a, m)
			if !math.IsNaN(v) {
				batch.Annotate(a.Ticker+"/"+string(m), strconv.FormatFloat(v, 'f', -1, 64))
			}
		}
	}
	for _, a := range cashMom.AssetList() {
		for _, m := range cashMom.MetricList() {
			v := cashMom.Value(a, m)
			if !math.IsNaN(v) {
				batch.Annotate(a.Ticker+"/"+string(m), strconv.FormatFloat(v, 'f', -1, 64))
			}
		}
	}

	// Count bad canary assets: those with non-positive momentum.
	badCanaryDF := canaryMom.CountWhere(data.AdjClose, func(v float64) bool {
		return math.IsNaN(v) || v <= 0
	})
	badCanary := int(badCanaryDF.Value(asset.Asset{Ticker: "COUNT"}, data.Count))

	// Compute cash fraction: CF = b/B (max 100%), with Easy Trading rounding.
	topT := s.TopT
	breadth := s.Breadth
	cf := math.Min(1.0, math.Floor(float64(badCanary)*float64(topT)/float64(breadth))/float64(topT))
	numRiskAssets := int(math.Round((1.0 - cf) * float64(topT)))

	// Record decision values.
	batch.Annotate("B", fmt.Sprintf("%d", badCanary))
	batch.Annotate("CF", fmt.Sprintf("%.2f", cf))
	batch.Annotate("T", fmt.Sprintf("%d", numRiskAssets))

	// Select top-T risk assets and best cash asset by momentum.
	portfolio.TopN(max(numRiskAssets, 1), data.AdjClose).Select(riskMom)
	portfolio.TopN(1, data.AdjClose).Select(cashMom)

	// Build allocation.
	members := make(map[asset.Asset]float64)

	if cf > 0 {
		for _, a := range cashMom.AssetList() {
			if cashMom.Value(a, portfolio.Selected) > 0 {
				members[a] = cf
				break
			}
		}
	}

	if numRiskAssets > 0 {
		riskWeight := (1.0 - cf) / float64(numRiskAssets)
		for _, a := range riskMom.AssetList() {
			if riskMom.Value(a, portfolio.Selected) > 0 {
				members[a] += riskWeight
			}
		}
	}

	justification := fmt.Sprintf("B=%d CF=%.0f%% T=%d", badCanary, cf*100, numRiskAssets)

	return batch.RebalanceTo(ctx, portfolio.Allocation{
		Date:          eng.CurrentDate(),
		Members:       members,
		Justification: justification,
	})
}

// momentum13612W computes the 13612W momentum score:
//
//	(12*ret1 + 4*ret3 + 2*ret6 + ret12) / 4
//
// where retN is the N-month total return.
func momentum13612W(df *data.DataFrame) *data.DataFrame {
	mom1 := df.Pct(1).MulScalar(12)
	mom3 := df.Pct(3).MulScalar(4)
	mom6 := df.Pct(6).MulScalar(2)
	mom12 := df.Pct(12)

	return mom1.Add(mom3).Add(mom6).Add(mom12).DivScalar(4)
}
