// frontend-client/src/components/Recommendations.tsx
import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';

interface Recommendation {
  id: string;
  title: string;
  city: string;
  property_type: string;
  price: number;
  surface: number;
  image?: string;
  recommendation_score: number;
  reliability_score: number;
}

const Recommendations: React.FC = () => {
  const [recommendations, setRecommendations] = useState<Recommendation[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchRecommendations();
  }, []);

  const fetchRecommendations = async () => {
    try {
      setLoading(true);
      const response = await fetch('/api/recommendations/personalized/?limit=12', {
        headers: {
          'Content-Type': 'application/json',
          ...(localStorage.getItem('access_token') && {
            'Authorization': `Bearer ${localStorage.getItem('access_token')}`
          })
        }
      });
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data = await response.json();
      setRecommendations(data.recommendations || []);
    } catch (error) {
      console.error('Error fetching recommendations:', error);
      setError(error instanceof Error ? error.message : 'Failed to load recommendations');
    } finally {
      setLoading(false);
    }
  };

  const handleClick = async (listingId: string) => {
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
          listing_id: listingId,
          referrer: 'recommendations'
        })
      });
    } catch (error) {
      console.error('Failed to track click:', error);
    }
  };

  if (loading) {
    return (
      <div className="recommendations-section">
        
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {[1, 2, 3, 4].map(i => (
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
      <div className="recommendations-section">
        <h2 className="text-2xl font-bold mb-6">Recommended For You</h2>
        <p className="text-gray-500 text-center py-8">Unable to load recommendations</p>
      </div>
    );
  }

  if (recommendations.length === 0) {
    return null;
  }

  return (
    <div className="recommendations-section">
      <h2 className="text-2xl font-bold mb-6">Recommended For You</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {recommendations.map((listing) => (
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
              {/* <div className="absolute top-2 right-2 bg-blue-500 text-white text-xs px-2 py-1 rounded-full">
                {Math.round(listing.recommendation_score * 100)}% match
              </div> */}
              {listing.reliability_score > 80 && (
                <div className="absolute bottom-2 left-2 bg-green-500 text-white text-xs px-2 py-1 rounded-full">
                  Verified
                </div>
              )}
            </div>
            <div className="p-4">
              <h3 className="font-semibold truncate">{listing.title}</h3>
              <p className="text-gray-600 text-sm">{listing.city}</p>
              <p className="text-blue-600 font-bold mt-2">
                {listing.price.toLocaleString()} TND
              </p>
              <div className="flex justify-between items-center mt-2 text-sm text-gray-500">
                <span className="capitalize">{listing.property_type}</span>
                <span>{listing.surface} m²</span>
              </div>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
};

export default Recommendations;