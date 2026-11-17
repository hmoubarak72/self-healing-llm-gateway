from __future__ import annotations

from app.config import ModelPricing


class CostCalculator:
    def __init__(self, pricing_table: dict[str, ModelPricing]) -> None:
        self.pricing_table = pricing_table

    def calculate_cost(self, model: str, prompt_tokens: int, completion_tokens: int) -> float:
        pricing = self.pricing_table.get(model)
        if pricing is None:
            pricing = ModelPricing(input_per_1k_usd=0.001, output_per_1k_usd=0.002)
        input_cost = (prompt_tokens / 1000.0) * pricing.input_per_1k_usd
        output_cost = (completion_tokens / 1000.0) * pricing.output_per_1k_usd
        return round(input_cost + output_cost, 8)
