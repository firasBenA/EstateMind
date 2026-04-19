// frontend-client/src/components/SimilarListings.tsx
import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';

interface SimilarListing {
  id: string;
  title: string;
  city: string;
  property_type: string;
  price: number;
  surface: number;
  image?: string;
  similarity_score: number;
  similarity_reason?: string;
}

interface SimilarListingsProps {
  listingId: string;
  limit?: number;
}

const SimilarListings: React.FC<SimilarListingsProps> = ({ listingId, limit = 6 }) => {
  const [similar, setSimilar] = useState<SimilarListing[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchSimilarListings();
  }, [listingId]);

  const fetchSimilarListings = async () => {
    try {
      setLoading(true);
      const response = await fetch(`/api/listings/${listingId}/similar/?limit=${limit}`, {
        headers: {
          'Content-Type': 'application/json',
          // Add auth token if needed
          ...(localStorage.getItem('access_token') && {
            'Authorization': `Bearer ${localStorage.getItem('access_token')}`
          })
        }
      });
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data = await response.json();
      setSimilar(data.results || []);
    } catch (error) {
      console.error('Error fetching similar listings:', error);
      setError(error instanceof Error ? error.message : 'Failed to load similar listings');
    } finally {
      setLoading(false);
    }
  };

  // Track click on similar listing
  const handleClick = async (clickedListingId: string) => {
    try {
      await fetch('/api/behaviors/track/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(localStorage.getItem('access_token') && {
            'Authorization': `Bearer ${localStorage.getItem('access_token')}`
          })
        },
        body: JSON.stringify({
          behavior_type: 'view',
          listing_id: clickedListingId,
          referrer: 'similar_listings',
          original_listing: listingId
        })
      });
    } catch (error) {
      console.error('Failed to track click:', error);
    }
  };

  if (loading) {
    return (
      <div className="similar-listings-section">
        <h3 className="text-xl font-bold mb-4">Similar Properties</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {[1, 2, 3].map(i => (
            <div key={i} className="border rounded-lg overflow-hidden animate-pulse">
              <div className="bg-gray-200 h-48"></div>
              <div className="p-4 space-y-2">
                <div className="bg-gray-200 h-4 rounded w-3/4"></div>
                <div className="bg-gray-200 h-3 rounded w-1/2"></div>
                <div className="bg-gray-200 h-5 rounded w-1/3"></div>
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="similar-listings-section">
        <h3 className="text-xl font-bold mb-4">Similar Properties</h3>
        <p className="text-gray-500 text-center py-8">Unable to load similar properties</p>
      </div>
    );
  }

  if (similar.length === 0) {
    return null;
  }

  return (
    <div className="similar-listings-section mt-8">
      <h3 className="text-xl font-bold mb-4">Similar Properties</h3>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {similar.map((listing) => (
          <Link
            key={listing.id}
            to={`/listing/${listing.id}`}
            onClick={() => handleClick(listing.id)}
            className="border rounded-lg overflow-hidden hover:shadow-lg transition group"
          >
            <div className="relative">
              <img
                src={listing.image || '/no-image.svg'}
                alt={listing.title}
                className="w-full h-48 object-cover group-hover:scale-105 transition duration-300"
                onError={(e) => {
                  (e.target as HTMLImageElement).src = '/no-image.svg';
                }}
              />
              <div className="absolute top-2 right-2 bg-black/60 text-white text-xs px-2 py-1 rounded-full">
                {Math.round(listing.similarity_score * 100)}% match
              </div>
            </div>
            <div className="p-4">
              <h4 className="font-semibold truncate">{listing.title}</h4>
              <p className="text-gray-600 text-sm">{listing.city}</p>
              <p className="text-blue-600 font-bold mt-2">
                {listing.price.toLocaleString()} TND
              </p>
              <div className="flex justify-between items-center mt-2 text-sm text-gray-500">
                <span className="capitalize">{listing.property_type}</span>
                <span>{listing.surface} m²</span>
              </div>
              {listing.similarity_reason && (
                <div className="mt-2 text-xs text-gray-400">
                  Match: {listing.similarity_reason.replace(/_/g, ' ')}
                </div>
              )}
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
};

export default SimilarListings;