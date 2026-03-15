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
	"math"
	"sort"
	"time"

	"github.com/penny-vault/pvbt/asset"
	"github.com/penny-vault/pvbt/data"
	"github.com/penny-vault/pvbt/engine"
	"github.com/penny-vault/pvbt/portfolio"
	"github.com/penny-vault/pvbt/tradecron"
	"github.com/penny-vault/pvbt/universe"
	"github.com/rs/zerolog"
)

//go:embed README.md
var description string

type DefensiveAssetAllocation struct {
	RiskUniverse       universe.Universe `pvbt:"risk-universe" desc:"List of ETF, Mutual Fund, or Stock tickers in the risk universe" default:"SPY,IWM,QQQ,VGK,EWJ,EEM,VNQ,DBC,GLD,TLT,HYG,LQD" suggest:"DAA-G12=SPY,IWM,QQQ,VGK,EWJ,EEM,VNQ,DBC,GLD,TLT,HYG,LQD|DAA-G6=SPY,VEA,VWO,LQD,TLT,HYG|DAA1-G4=SPY,VEA,VWO,AGG|DAA1-G12=SPY,IWM,QQQ,VGK,EWJ,EEM,VNQ,DBC,GLD,TLT,HYG,LQD|DAA1-U1=SPY"`
	ProtectiveUniverse universe.Universe `pvbt:"protective-universe" desc:"Canary universe that signals when to shift to cash" default:"EEM,AGG" suggest:"DAA-G12=EEM,AGG|DAA-G6=EEM,AGG|DAA1-G4=EEM,AGG|DAA1-G12=EEM,AGG|DAA1-U1=EEM,AGG"`
	CashUniverse       universe.Universe `pvbt:"cash-universe" desc:"Safe-haven assets for defensive allocation" default:"SHY,IEF,LQD" suggest:"DAA-G12=SHY,IEF,LQD|DAA-G6=SHY,IEF,LQD|DAA1-G4=SHV,IEF,UST|DAA1-G12=SHV,IEF,UST|DAA1-U1=SHV,IEF,UST"`
	Breadth            int               `pvbt:"breadth" desc:"Breadth parameter determining cash fraction from canary signals" default:"2" suggest:"DAA-G12=2|DAA-G6=2|DAA1-G4=1|DAA1-G12=1|DAA1-U1=1"`
	TopT               int               `pvbt:"top-t" desc:"Number of top risk assets to invest in" default:"6" suggest:"DAA-G12=6|DAA-G6=6|DAA1-G4=4|DAA1-G12=2|DAA1-U1=1"`
}

func (s *DefensiveAssetAllocation) Name() string {
	return "Defensive Asset Allocation"
}

func (s *DefensiveAssetAllocation) Setup(e *engine.Engine) {
	tc, err := tradecron.New("@monthend", tradecron.MarketHours{Open: 930, Close: 1600})
	if err != nil {
		panic(err)
	}
	e.Schedule(tc)
	e.SetBenchmark(e.Asset("VFINX"))
	e.RiskFreeAsset(e.Asset("DGS3MO"))
}

func (s *DefensiveAssetAllocation) Describe() engine.StrategyDescription {
	return engine.StrategyDescription{
		ShortCode:   "daa",
		Description: description,
		Source:      "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3212862",
		Version:     "1.0.1",
		VersionDate: time.Date(2026, 3, 14, 0, 0, 0, 0, time.UTC),
	}
}

func (s *DefensiveAssetAllocation) Compute(ctx context.Context, e *engine.Engine, p portfolio.Portfolio) {
	log := zerolog.Ctx(ctx)

	// 1. Fetch 12-month window of monthly close prices for all universes combined.
	riskDF, err := s.RiskUniverse.Window(ctx, portfolio.Months(12), data.MetricClose)
	if err != nil {
		log.Error().Err(err).Msg("failed to fetch risk universe prices")
		return
	}

	protectiveDF, err := s.ProtectiveUniverse.Window(ctx, portfolio.Months(12), data.MetricClose)
	if err != nil {
		log.Error().Err(err).Msg("failed to fetch protective universe prices")
		return
	}

	cashDF, err := s.CashUniverse.Window(ctx, portfolio.Months(12), data.MetricClose)
	if err != nil {
		log.Error().Err(err).Msg("failed to fetch cash universe prices")
		return
	}

	// 2. Downsample to monthly frequency (use last value in each month).
	riskMonthly := riskDF.Downsample(data.Monthly).Last()
	protectiveMonthly := protectiveDF.Downsample(data.Monthly).Last()
	cashMonthly := cashDF.Downsample(data.Monthly).Last()

	// Need at least 13 rows for Pct(12) to produce a valid value.
	if riskMonthly.Len() < 13 || protectiveMonthly.Len() < 13 || cashMonthly.Len() < 13 {
		return
	}

	// 3. Compute Momentum12631 for each asset.
	//    momentum = (Pct(1)*12 + Pct(3)*4 + Pct(6)*2 + Pct(12)*1) / 4
	riskMom := momentum12631(riskMonthly)
	protectiveMom := momentum12631(protectiveMonthly)
	cashMom := momentum12631(cashMonthly)

	// Take the last row from each momentum DataFrame.
	riskMom = riskMom.Drop(math.NaN()).Last()
	protectiveMom = protectiveMom.Drop(math.NaN()).Last()
	cashMom = cashMom.Drop(math.NaN()).Last()

	if riskMom.Len() == 0 || protectiveMom.Len() == 0 || cashMom.Len() == 0 {
		return
	}

	// 4. Count bad canary assets (B): protective assets with negative momentum.
	badCanary := 0
	for _, a := range protectiveMom.AssetList() {
		val := protectiveMom.Value(a, data.MetricClose)
		if val < 0 {
			badCanary++
		}
	}

	// 5. Compute cash fraction: CF = min(1.0, floor(B * topT / breadth) / topT)
	topT := s.TopT
	breadth := s.Breadth
	cf := math.Min(1.0, math.Floor(float64(badCanary)*float64(topT)/float64(breadth))/float64(topT))

	// 6. Compute T = round((1 - CF) * topT)
	t := int(math.Round((1.0 - cf) * float64(topT)))

	// 7. Select top-T risk assets by momentum score.
	type assetScore struct {
		a     asset.Asset
		score float64
	}
	var riskScores []assetScore
	for _, a := range riskMom.AssetList() {
		riskScores = append(riskScores, assetScore{a: a, score: riskMom.Value(a, data.MetricClose)})
	}
	sort.Slice(riskScores, func(i, j int) bool {
		return riskScores[i].score > riskScores[j].score
	})
	if t > len(riskScores) {
		t = len(riskScores)
	}
	topRisk := riskScores[:t]

	// 8. Select highest-momentum cash asset.
	var bestCash asset.Asset
	bestCashScore := math.Inf(-1)
	for _, a := range cashMom.AssetList() {
		val := cashMom.Value(a, data.MetricClose)
		if val > bestCashScore {
			bestCashScore = val
			bestCash = a
		}
	}

	// 9. Build allocation: cash asset gets CF weight, T risk assets split (1-CF)/T each.
	members := make(map[asset.Asset]float64)
	if cf > 0 && bestCash != (asset.Asset{}) {
		members[bestCash] = cf
	}
	if t > 0 {
		riskWeight := (1.0 - cf) / float64(t)
		for _, rs := range topRisk {
			members[rs.a] += riskWeight
		}
	}

	allocation := portfolio.Allocation{
		Date:    e.CurrentDate(),
		Members: members,
	}

	// 10. Rebalance to this allocation.
	if err := p.RebalanceTo(ctx, allocation); err != nil {
		log.Error().Err(err).Msg("rebalance failed")
	}
}

// momentum12631 computes the Momentum12631 score:
//
//	(Pct(1)*12 + Pct(3)*4 + Pct(6)*2 + Pct(12)*1) / 4
func momentum12631(df *data.DataFrame) *data.DataFrame {
	mom1 := df.Pct(1).MulScalar(12)
	mom3 := df.Pct(3).MulScalar(4)
	mom6 := df.Pct(6).MulScalar(2)
	mom12 := df.Pct(12)
	return mom1.Add(mom3).Add(mom6).Add(mom12).DivScalar(4)
}
