#!/usr/bin/env python3
"""
implied_vol.py  –  Black-Scholes implied-volatility solver
  * Newton–Raphson with Vega slope
  * Brent-bisection fallback
  * Includes dividend yield q (defaults to 0.0)
"""

import math
import sys
import os
from datetime import datetime, date
# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.database import Database

# ---------- standard-normal helpers ----------
SQRT_2PI = math.sqrt(2.0 * math.pi)
phi = lambda x: math.exp(-0.5 * x * x) / SQRT_2PI      # PDF
Phi = lambda x: 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))  # CDF


# ---------- Black-Scholes price ----------
def bs_price(S, K, r, q, T, sigma, cp):
    """Call (+1) or put (−1) price with continuous q."""
    if sigma <= 0.0 or T <= 0.0:
        return max(cp * (S - K), 0.0)
    sqT = math.sqrt(T)
    d1  = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqT)
    d2  = d1 - sigma * sqT
    disc_q, disc_r = math.exp(-q * T), math.exp(-r * T)
    if cp == +1:   # call
        return S * disc_q * Phi(d1) - K * disc_r * Phi(d2)
    else:          # put
        return K * disc_r * Phi(-d2) - S * disc_q * Phi(-d1)


# ---------- Vega ----------
def bs_vega(S, r, q, T, d1):
    return S * math.exp(-q * T) * phi(d1) * math.sqrt(T)


# ---------- Implied-vol solver ----------
def implied_vol(S, K, r, T, P_obs, cp, q=0.0,
                tol_price=1e-6, tol_sigma=1e-7, max_iter=100):
    sigma = 0.20                               # initial guess 20 %
    for _ in range(max_iter):                  # Newton loop
        sqT = math.sqrt(T)
        d1  = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqT)
        price = bs_price(S, K, r, q, T, sigma, cp)
        diff  = price - P_obs                  # f(σ)
        if abs(diff) < tol_price:
            return sigma
        vega = bs_vega(S, r, q, T, d1)         # f′(σ)
        if vega < 1e-12:
            break                              # fallback if slope ~ 0
        step  = diff / vega
        sigma -= step
        if abs(step) < tol_sigma:
            return sigma

    # Brent fallback (guaranteed root in [1e-6, 5])
    low, high = 1e-6, 5.0
    f_low  = bs_price(S, K, r, q, T, low,  cp) - P_obs
    f_high = bs_price(S, K, r, q, T, high, cp) - P_obs
    if f_low * f_high >= 0:
        raise RuntimeError("Root not bracketed; check inputs.")
    for _ in range(200):
        mid = 0.5 * (low + high)
        f_mid = bs_price(S, K, r, q, T, mid, cp) - P_obs
        if abs(f_mid) < tol_price:
            return mid
        if f_low * f_mid < 0:
            high, f_high = mid, f_mid
        else:
            low,  f_low  = mid, f_mid
    raise RuntimeError("Brent method failed to converge.")

def simple_to_continuous(entry_date):
    db = Database()
    with db:
        r_simple = db.get_risk_free_rate(entry_date) / 100.0
        return math.log(1.0 + r_simple)

def get_q(ticker, entry_date, exit_date, r, S, T):
    
    db = Database()
    with db:
        div_rows = db.get_dividends(
        ticker=ticker,
        start_date=entry_date,
        end_date=exit_date
    )

    # No cash dividends ⇒ q = 0
    if not div_rows:
        return 0.0
    if T <= 0.0:
        raise ValueError("exit_date must be after entry_date")

    # ------ discounted PV of all dividends -----------------------------------
    pv_total = 0.0
    for row in div_rows:
        # Accept either tuple or dict
        ex_div_date  = row[1] if isinstance(row, tuple) else row["ex_div_date"]
        cash_amount  = row[2] if isinstance(row, tuple) else row["cash_amount"]

        # Convert the string date to datetime object
        ex_div_date = datetime.strptime(ex_div_date, "%Y-%m-%d").date()
        # Now you can subtract them
        t_i = (ex_div_date - entry_date).days / 365.0
        pv_total += float(cash_amount) * math.exp(-r * t_i)

    # ------ continuous dividend yield q --------------------------------------
    # Guard against "dividend bigger than price" edge-case
    if pv_total >= S:
        raise ValueError("PV of dividends >= spot price; check inputs")

    q = (1.0 / T) * math.log(S / (S - pv_total))

    return q
    
def get_delta(S, K, r, T, cp, q, sigma):
    if T <= 0 or sigma <= 0:
        # Option has expired or is effectively intrinsic only
        return max(cp * (1.0 if S > K else 0.0), 0.0) * cp
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    delta = cp * Phi(cp * d1)
    return delta
    #Γ (Gamma) = Se^(-qT) * φ(d₁) / (S*σ*√T)
def get_gamma(S, K, r, T, q, sigma):
    if sigma <= 0 or T <= 0:
        return 0.0  # Gamma is undefined for zero volatility or zero time to expiration
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    phi_d1 = math.exp(-0.5 * d1 ** 2) / math.sqrt(2 * math.pi)
    gamma = math.exp(-q * T) * phi_d1 / (S * sigma * math.sqrt(T))
    return gamma

#Θ (Theta) = -(S*σ*e^(-qT) * φ(d₁))/(2*√T) - cp*(r*K*e^(-rT)*Φ(cp*d₂) - q*S*e^(-qT)*Φ(cp*d₁))/365
def get_theta(S, K, r, T, q, sigma, cp, per_day=True):
    if sigma <= 0 or T <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r - q + 0.5*sigma**2)*T) / (sigma*math.sqrt(T))
    d2 = d1 - sigma*math.sqrt(T)

    phi_d1 = math.exp(-0.5*d1**2) / math.sqrt(2*math.pi)       # PDF
    Phi_d1 = Phi(cp*d1)                                        # CDF with sign
    Phi_d2 = Phi(cp*d2)

    theta_annual = (-(S * sigma * math.exp(-q*T) * phi_d1) / (2*math.sqrt(T))
                    - cp * (r*K*math.exp(-r*T) * Phi_d2
                            - q*S*math.exp(-q*T) * Phi_d1))

    return theta_annual / 365.0 if per_day else theta_annual

#Vega = S*e^(-qT) * φ(d₁) * √T/100
def get_vega(S, K, r, T, q, sigma):
    if sigma <= 0 or T <= 0:
        return 0.0  # Vega is undefined for zero volatility or zero time to expiration
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    phi_d1 = math.exp(-0.5 * d1 ** 2) / math.sqrt(2 * math.pi)
    vega = S*math.exp(-q*T)*phi_d1*math.sqrt(T)/100
    return vega

def get_greeks(S, K, P_obs, cp, entry_date, exit_date, ticker):
    """
    Get the implied volatility and delta of an option
    S - spot price
    K - strike price
    P_obs - observed price
    cp - call or put
    entry_date - entry date (string in YYYY-MM-DD format)
    exit_date - exit date (date object)
    ticker - ticker
    """
    # Convert entry_date string to date object if needed
    if isinstance(entry_date, str):
        entry_date = datetime.strptime(entry_date, "%Y-%m-%d").date()
    
    r = simple_to_continuous(entry_date)
    T_days = (exit_date - entry_date).days
    T_years = T_days / 365
    q = get_q(ticker, entry_date, exit_date, r, S, T_years)
    iv = implied_vol(S, K, r, T_years, P_obs, cp, q=q)
    delta = get_delta(S, K, r, T_years, cp, q, iv)
    gamma = get_gamma(S, K, r, T_years, q, iv)
    theta = get_theta(S, K, r, T_years, q, iv, cp)
    vega = get_vega(S, K, r, T_years, q, iv)
    return iv, delta, gamma, theta, vega
    
# ---------- quick demo ----------
if __name__ == "__main__":
    S, K   = 201.10, 210.00

    T_days = 30        # how long until expiration
    T_years = T_days / 365

    entry_date = date(2025, 3, 28)      # date object instead of datetime
    exit_date = date(2025, 4, 3)        # date object instead of datetime
    r = 0.0433#simple_to_continuous(entry_date)
    q = 0.0 #get_q("A", entry_date, exit_date, r, S, T_years)
    print(f"Dividend yield q = {q:.4%}")
    P_mkt  = 3.4           # observed call price
    cp     = +1              # +1 call, −1 put

    iv = implied_vol(S, K, r, T_years, P_mkt, cp, q=q)
    print(f"Implied volatility = {iv:.4%}")
    delta = get_delta(S, K, r, T_years, cp, q, iv)
    print(f"Delta = {delta}")
    gamma = get_gamma(S, K, r, T_years, q, iv)
    print(f"Gamma = {gamma}")
    theta = get_theta(S, K, r, T_years, q, iv, cp)
    print(f"Theta = {theta}")
    vega = get_vega(S, K, r, T_years, q, iv)
    print(f"Vega = {vega}")
