"""
Snapshot builder - core logic to assemble performance snapshots.
Combines data from Google Ads and Merchant Center APIs.
"""
from typing import List, Optional
import random
from datetime import datetime, timedelta

from config import settings
from app.models import (
    Summary, Issue, RecommendedAction, SnapshotResponse,
    CampaignKPI, SearchTermData, DisapprovedProduct
)
from app.google_ads import (
    ads_get_account_kpis,
    ads_get_campaign_kpis,
    ads_get_search_terms,
    ads_get_policy_issues
)
from app.merchant_center import mc_get_disapproved_products, mc_check_feed_health


def build_snapshot(customer_id: str, date_range: str = "7d") -> SnapshotResponse:
    """
    Build a complete performance snapshot for the given customer and date range.
    
    Args:
        customer_id: Google Ads customer ID (without hyphens)
        date_range: Date range for the snapshot (e.g., '7d', '30d')
    
    Returns:
        SnapshotResponse with summary, issues, and recommended actions
    """
    # Check if we should run in demo mode (no developer token)
    if not settings.google_ads_developer_token:
        return build_demo_snapshot(customer_id, date_range)

    try:
        # Fetch data from Google Ads
        account_kpis = ads_get_account_kpis(customer_id, date_range)
        campaign_kpis = ads_get_campaign_kpis(customer_id, date_range)
        search_terms = ads_get_search_terms(customer_id, date_range, min_spend=10.0)
        policy_issues = ads_get_policy_issues(customer_id)
        
        # Fetch data from Merchant Center
        disapproved_products = mc_get_disapproved_products()
        
        # Build summary
        summary = Summary(
            date_range=date_range,
            total_spend=account_kpis["total_spend"],
            total_conversions=account_kpis["total_conversions"],
            average_cpa=account_kpis["average_cpa"],
            roas=account_kpis["roas"],
            currency=account_kpis["currency"]
        )
        
        # Identify issues and generate recommendations
        issues = []
        recommendations = []
        
        # 1. Check for disapproved products
        issues_from_products, recs_from_products = analyze_disapproved_products(disapproved_products)
        issues.extend(issues_from_products)
        recommendations.extend(recs_from_products)
        
        # 2. Check for poor-performing campaigns
        issues_from_campaigns, recs_from_campaigns = analyze_campaigns(campaign_kpis, account_kpis)
        issues.extend(issues_from_campaigns)
        recommendations.extend(recs_from_campaigns)
        
        # 3. Check for wasteful search terms
        issues_from_search_terms, recs_from_search_terms = analyze_search_terms(search_terms)
        issues.extend(issues_from_search_terms)
        recommendations.extend(recs_from_search_terms)
        
        # 4. Check for policy-limited ads
        issues_from_policy, recs_from_policy = analyze_policy_issues(policy_issues)
        issues.extend(issues_from_policy)
        recommendations.extend(recs_from_policy)
        
        # Sort issues by severity
        severity_order = {"high": 0, "medium": 1, "low": 2}
        issues.sort(key=lambda x: severity_order.get(x.severity, 3))
        
        # Sort recommendations by priority
        priority_order = {"high": 0, "medium": 1, "low": 2}
        recommendations.sort(key=lambda x: priority_order.get(x.priority, 3))
        
        return SnapshotResponse(
            summary=summary,
            top_issues=issues,
            recommended_actions=recommendations
        )
    except Exception as e:
        # Fallback to demo mode if API calls fail
        print(f"API Error: {e}. Falling back to demo mode.")
        return build_demo_snapshot(customer_id, date_range)


def build_demo_snapshot(customer_id: str, date_range: str) -> SnapshotResponse:
    """Generate a demo snapshot with mock data."""
    
    # Mock Summary
    summary = Summary(
        date_range=date_range,
        total_spend=1250.50,
        total_conversions=45.0,
        average_cpa=27.79,
        roas=3.2,
        currency="USD"
    )
    
    issues = []
    recommendations = []
    
    # Mock Issue 1: Disapproved Product
    issues.append(Issue(
        type="disapproved_product",
        severity="high",
        description="Product 'Premium Wireless Headphones' (ID: 5521) is disapproved: Invalid GTIN",
        metadata={
            "product_id": "5521",
            "product_title": "Premium Wireless Headphones",
            "issues": ["Invalid GTIN"]
        }
    ))
    
    recommendations.append(RecommendedAction(
        action_type="fix_product_feed",
        description="Fix product data for 'Premium Wireless Headphones' and request a review in Merchant Center",
        priority="high",
        related_issue_type="disapproved_product"
    ))
    
    # Mock Issue 2: Zero Conversion Campaign
    issues.append(Issue(
        type="zero_conversion_campaign",
        severity="high",
        description="Campaign 'Display - Retargeting' spent $150.00 with 0 conversions",
        metadata={
            "campaign_id": "998877",
            "campaign_name": "Display - Retargeting",
            "spend": 150.00,
            "conversions": 0
        }
    ))
    
    recommendations.append(RecommendedAction(
        action_type="optimize_campaign",
        description="Review and optimize campaign 'Display - Retargeting' or pause it to prevent budget waste",
        priority="high",
        related_issue_type="zero_conversion_campaign"
    ))
    
    # Mock Issue 3: Wasteful Search Term
    issues.append(Issue(
        type="wasteful_search_term",
        severity="medium",
        description="Search term 'free headphones' spent $45.20 with 0 conversions",
        metadata={
            "search_term": "free headphones",
            "cost": 45.20,
            "clicks": 32,
            "conversions": 0
        }
    ))
    
    recommendations.append(RecommendedAction(
        action_type="add_negative_keyword",
        description="Add 'free headphones' as a negative keyword to prevent budget waste",
        priority="medium",
        related_issue_type="wasteful_search_term"
    ))
    
    return SnapshotResponse(
        summary=summary,
        top_issues=issues,
        recommended_actions=recommendations
    )


def analyze_disapproved_products(
    disapproved_products: List[DisapprovedProduct]
) -> tuple[List[Issue], List[RecommendedAction]]:
    """
    Analyze disapproved products and generate issues and recommendations.
    """
    issues = []
    recommendations = []
    
    for product in disapproved_products[:10]:  # Limit to top 10
        issue_desc = f"Product '{product.title}' (ID: {product.product_id}) is disapproved"
        if product.issues:
            issue_desc += f": {product.issues[0]}"
        
        issues.append(Issue(
            type="disapproved_product",
            severity="high",
            description=issue_desc,
            metadata={
                "product_id": product.product_id,
                "product_title": product.title,
                "issues": product.issues
            }
        ))
        
        recommendations.append(RecommendedAction(
            action_type="fix_product_feed",
            description=f"Fix product data for '{product.title}' and request a review in Merchant Center",
            priority="high",
            related_issue_type="disapproved_product"
        ))
    
    return issues, recommendations


def analyze_campaigns(
    campaign_kpis: List[CampaignKPI],
    account_kpis: dict
) -> tuple[List[Issue], List[RecommendedAction]]:
    """
    Analyze campaign performance and identify issues.
    """
    issues = []
    recommendations = []
    
    avg_account_cpa = account_kpis.get("average_cpa")
    
    for campaign in campaign_kpis:
        # Check for campaigns with zero conversions and significant spend
        if campaign.conversions == 0 and campaign.spend > 50:
            issues.append(Issue(
                type="zero_conversion_campaign",
                severity="high",
                description=f"Campaign '{campaign.campaign_name}' spent ${campaign.spend:.2f} with 0 conversions",
                metadata={
                    "campaign_id": campaign.campaign_id,
                    "campaign_name": campaign.campaign_name,
                    "spend": campaign.spend,
                    "conversions": campaign.conversions
                }
            ))
            
            recommendations.append(RecommendedAction(
                action_type="optimize_campaign",
                description=f"Review and optimize campaign '{campaign.campaign_name}' or pause it to prevent budget waste",
                priority="high",
                related_issue_type="zero_conversion_campaign"
            ))
        
        # Check for campaigns with high CPA
        elif campaign.cpa and avg_account_cpa and campaign.cpa > avg_account_cpa * 2:
            issues.append(Issue(
                type="high_cpa_campaign",
                severity="medium",
                description=f"Campaign '{campaign.campaign_name}' has CPA of ${campaign.cpa:.2f}, 2x higher than account average",
                metadata={
                    "campaign_id": campaign.campaign_id,
                    "campaign_name": campaign.campaign_name,
                    "cpa": campaign.cpa,
                    "account_avg_cpa": avg_account_cpa
                }
            ))
            
            recommendations.append(RecommendedAction(
                action_type="optimize_campaign",
                description=f"Optimize targeting or ad copy for campaign '{campaign.campaign_name}' to reduce CPA",
                priority="medium",
                related_issue_type="high_cpa_campaign"
            ))
    
    return issues, recommendations


def analyze_search_terms(
    search_terms: List[SearchTermData]
) -> tuple[List[Issue], List[RecommendedAction]]:
    """
    Analyze search terms and identify wasteful queries.
    """
    issues = []
    recommendations = []
    
    for term in search_terms[:15]:  # Limit to top 15
        # Check for high-cost, zero-conversion search terms
        if term.conversions == 0 and term.cost > 20:
            issues.append(Issue(
                type="wasteful_search_term",
                severity="medium",
                description=f"Search term '{term.search_term}' spent ${term.cost:.2f} with 0 conversions",
                metadata={
                    "search_term": term.search_term,
                    "cost": term.cost,
                    "clicks": term.clicks,
                    "conversions": term.conversions
                }
            ))
            
            recommendations.append(RecommendedAction(
                action_type="add_negative_keyword",
                description=f"Add '{term.search_term}' as a negative keyword to prevent budget waste",
                priority="medium",
                related_issue_type="wasteful_search_term"
            ))
        
        # Check for low conversion rate with significant spend
        elif term.conversion_rate and term.conversion_rate < 1.0 and term.cost > 50:
            issues.append(Issue(
                type="low_conversion_search_term",
                severity="low",
                description=f"Search term '{term.search_term}' has low conversion rate ({term.conversion_rate:.2f}%) with ${term.cost:.2f} spend",
                metadata={
                    "search_term": term.search_term,
                    "cost": term.cost,
                    "conversion_rate": term.conversion_rate
                }
            ))
            
            recommendations.append(RecommendedAction(
                action_type="review_search_term",
                description=f"Review search term '{term.search_term}' and consider adding as negative or adjusting match type",
                priority="low",
                related_issue_type="low_conversion_search_term"
            ))
    
    return issues, recommendations


def analyze_policy_issues(
    policy_issues: List[dict]
) -> tuple[List[Issue], List[RecommendedAction]]:
    """
    Analyze policy-limited or disapproved ads.
    """
    issues = []
    recommendations = []
    
    for policy_issue in policy_issues[:10]:  # Limit to top 10
        issues.append(Issue(
            type="policy_limited_ad",
            severity="medium",
            description=f"Ad '{policy_issue['ad_name']}' in campaign '{policy_issue['campaign_name']}' has approval status: {policy_issue['approval_status']}",
            metadata=policy_issue
        ))
        
        recommendations.append(RecommendedAction(
            action_type="fix_policy_issue",
            description=f"Review and modify ad '{policy_issue['ad_name']}' to comply with Google Ads policies",
            priority="medium",
            related_issue_type="policy_limited_ad"
        ))
    
    return issues, recommendations
