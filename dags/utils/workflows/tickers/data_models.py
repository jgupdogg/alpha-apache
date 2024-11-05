# dags/helpers/data_models.py

from dataclasses import dataclass
from typing import List

@dataclass
class Profile:
    symbol: str
    price: float
    beta: float
    volAvg: int
    mktCap: int
    lastDiv: float
    range: str
    changes: float
    companyName: str
    currency: str
    cik: str
    isin: str
    cusip: str
    exchange: str
    exchangeShortName: str
    industry: str
    website: str
    description: str
    ceo: str
    sector: str
    country: str
    fullTimeEmployees: str
    phone: str
    address: str
    city: str
    state: str
    zip: str
    dcfDiff: float
    dcf: float
    image: str
    ipoDate: str
    defaultImage: bool
    isEtf: bool
    isActivelyTrading: bool
    isAdr: bool
    isFund: bool

@dataclass
class Metrics:
    dividendYielTTM: float
    volume: int
    yearHigh: float
    yearLow: float

@dataclass
class Ratios:
    dividendYielTTM: float
    dividendYielPercentageTTM: float
    peRatioTTM: float
    pegRatioTTM: float
    payoutRatioTTM: float
    currentRatioTTM: float
    quickRatioTTM: float
    cashRatioTTM: float
    daysOfSalesOutstandingTTM: float
    daysOfInventoryOutstandingTTM: float
    operatingCycleTTM: float
    daysOfPayablesOutstandingTTM: float
    cashConversionCycleTTM: float
    grossProfitMarginTTM: float
    operatingProfitMarginTTM: float
    pretaxProfitMarginTTM: float
    netProfitMarginTTM: float
    effectiveTaxRateTTM: float
    returnOnAssetsTTM: float
    returnOnEquityTTM: float
    returnOnCapitalEmployedTTM: float
    netIncomePerEBTTTM: float
    ebtPerEbitTTM: float
    ebitPerRevenueTTM: float
    debtRatioTTM: float
    debtEquityRatioTTM: float
    longTermDebtToCapitalizationTTM: float
    totalDebtToCapitalizationTTM: float
    interestCoverageTTM: float
    cashFlowToDebtRatioTTM: float
    companyEquityMultiplierTTM: float
    receivablesTurnoverTTM: float
    payablesTurnoverTTM: float
    inventoryTurnoverTTM: float
    fixedAssetTurnoverTTM: float
    assetTurnoverTTM: float
    operatingCashFlowPerShareTTM: float
    freeCashFlowPerShareTTM: float
    cashPerShareTTM: float
    operatingCashFlowSalesRatioTTM: float
    freeCashFlowOperatingCashFlowRatioTTM: float
    cashFlowCoverageRatiosTTM: float
    shortTermCoverageRatiosTTM: float
    capitalExpenditureCoverageRatioTTM: float
    dividendPaidAndCapexCoverageRatioTTM: float
    priceBookValueRatioTTM: float
    priceToBookRatioTTM: float
    priceToSalesRatioTTM: float
    priceEarningsRatioTTM: float
    priceToFreeCashFlowsRatioTTM: float
    priceToOperatingCashFlowsRatioTTM: float
    priceCashFlowRatioTTM: float
    priceEarningsToGrowthRatioTTM: float
    priceSalesRatioTTM: float
    enterpriseValueMultipleTTM: float
    priceFairValueTTM: float
    dividendPerShareTTM: float

@dataclass
class InsideTrade:
    symbol: str
    filingDate: str
    transactionDate: str
    reportingCik: str
    transactionType: str
    securitiesOwned: int
    companyCik: str
    reportingName: str
    typeOfOwner: str
    acquisitionOrDisposition: str
    formType: str
    securitiesTransacted: int
    price: float
    securityName: str
    link: str

@dataclass
class SplitHistory:
    symbol: str
    date: str
    label: str
    numerator: int
    denominator: int

@dataclass
class StockDividend:
    symbol: str
    date: str
    label: str
    adjDividend: float
    dividend: float
    recordDate: str
    paymentDate: str
    declarationDate: str

@dataclass
class StockNews:
    symbol: str
    publishedDate: str
    title: str
    image: str
    site: str
    text: str
    url: str
    category: str  # Added category field

@dataclass
class PressRelease:
    symbol: str
    date: str
    title: str
    text: str
    category: str  # Will be set to 'press'
