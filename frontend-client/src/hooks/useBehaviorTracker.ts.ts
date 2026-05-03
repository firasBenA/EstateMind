// frontend-client/src/hooks/useBehaviorTracker.ts
import { useCallback } from 'react';
import { useAuth } from '@/lib/auth-context';

interface TrackOptions {
  duration?: number;
  referrer?: string;
  searchQuery?: string;
  filters?: Record<string, any>;
}

export function useBehaviorTracker() {
  const { isAuthenticated } = useAuth();

  const track = useCallback(async (
    behaviorType: 'view' | 'search_click' | 'save' | 'favorite' | 'contact',
    listingId: string,
    options?: TrackOptions
  ) => {
    // Only track if user is authenticated
    if (!isAuthenticated) return;

    try {
      await fetch('/api/behaviors/track/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${localStorage.getItem('access_token')}`
        },
        body: JSON.stringify({
          behavior_type: behaviorType,
          listing_id: listingId,
          duration_seconds: options?.duration || 0,
          referrer: options?.referrer || 'direct',
          search_query: options?.searchQuery,
          filters: options?.filters
        })
      });
    } catch (error) {
      console.error('Failed to track behavior:', error);
    }
  }, [isAuthenticated]);

  const trackSearch = useCallback(async (
    searchQuery: string,
    filters: Record<string, any>,
    resultsCount: number,
    clickedListingId?: string
  ) => {
    if (!isAuthenticated) return;

    try {
      await fetch('/api/behaviors/track-search/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${localStorage.getItem('access_token')}`
        },
        body: JSON.stringify({
          search_query: searchQuery,
          filters: filters,
          results_count: resultsCount,
          clicked_listing_id: clickedListingId
        })
      });
    } catch (error) {
      console.error('Failed to track search:', error);
    }
  }, [isAuthenticated]);

  return { track, trackSearch };
}