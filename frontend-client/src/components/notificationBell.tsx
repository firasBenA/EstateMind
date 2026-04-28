// frontend-client/src/components/NotificationBell.tsx
import React, { useState, useEffect, useCallback } from 'react';
import { Link } from 'react-router-dom';

interface Notification {
  id: number;
  type: 'new_listing' | 'price_drop' | 'similar_listing';
  title: string;
  message: string;
  listing_id: string | null;
  is_read: boolean;
  created_at: string;
  data?: Record<string, any>;
}

// Declare Pusher type for TypeScript
declare global {
  interface Window {
    Pusher: any;
  }
}

const NotificationBell: React.FC = () => {
  const [notifications, setNotifications] = useState<Notification[]>([]);
  const [unreadCount, setUnreadCount] = useState(0);
  const [isOpen, setIsOpen] = useState(false);
  const [pusher, setPusher] = useState<any>(null);

  // Request notification permission on mount
  useEffect(() => {
    if ('Notification' in window && Notification.permission === 'default') {
      Notification.requestPermission();
    }
  }, []);

  // Fetch notifications
  const fetchNotifications = useCallback(async () => {
    try {
      const response = await fetch('/api/notifications/?limit=20', {
        headers: {
          'Content-Type': 'application/json',
          ...(localStorage.getItem('access_token') && {
            'Authorization': `Bearer ${localStorage.getItem('access_token')}`
          })
        }
      });
      
      if (!response.ok) throw new Error('Failed to fetch notifications');
      
      const data = await response.json();
      setNotifications(data.notifications || []);
      setUnreadCount(data.unread_count || 0);
    } catch (error) {
      console.error('Error fetching notifications:', error);
    }
  }, []);

  // Initialize Pusher
  useEffect(() => {
    // Dynamically import Pusher
    const initPusher = async () => {
      try {
        // Load Pusher from CDN if not already loaded
        if (!window.Pusher) {
          const script = document.createElement('script');
          script.src = 'https://js.pusher.com/8.2.0/pusher.min.js';
          script.async = true;
          document.head.appendChild(script);
          
          await new Promise((resolve) => {
            script.onload = resolve;
          });
        }
        
        const userId = localStorage.getItem('userId');
        if (!userId || !process.env.REACT_APP_PUSHER_KEY) {
          console.warn('Missing Pusher config or user ID');
          return;
        }
        
        const pusherInstance = new window.Pusher(process.env.REACT_APP_PUSHER_KEY, {
          cluster: process.env.REACT_APP_PUSHER_CLUSTER || 'eu',
          authEndpoint: '/api/pusher/auth',
        });
        
        const channel = pusherInstance.subscribe(`user-${userId}`);
        
        channel.bind('new-listing', (data: any) => {
          addNotification(data);
        });
        
        channel.bind('price-drop', (data: any) => {
          addNotification(data);
        });
        
        channel.bind('similar-listing', (data: any) => {
          addNotification(data);
        });
        
        setPusher(pusherInstance);
      } catch (error) {
        console.error('Failed to initialize Pusher:', error);
      }
    };
    
    initPusher();
    fetchNotifications();
    
    return () => {
      if (pusher) {
        pusher.disconnect();
      }
    };
  }, [fetchNotifications]);

  const addNotification = (notification: any) => {
    const newNotification: Notification = {
      id: notification.id || Date.now(),
      type: notification.type,
      title: notification.title,
      message: notification.message,
      listing_id: notification.listing_id,
      is_read: false,
      created_at: notification.created_at || new Date().toISOString(),
      data: notification.data
    };
    
    setNotifications(prev => [newNotification, ...prev]);
    setUnreadCount(prev => prev + 1);
    
    // Show browser notification
    if (Notification.permission === 'granted') {
      new Notification(notification.title, {
        body: notification.message,
        icon: '/logo192.png',
        tag: notification.id?.toString(),
      });
    }
  };

  const markAsRead = async (notificationId: number) => {
    try {
      const response = await fetch(`/api/notifications/${notificationId}/read/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(localStorage.getItem('access_token') && {
            'Authorization': `Bearer ${localStorage.getItem('access_token')}`
          })
        }
      });
      
      if (!response.ok) throw new Error('Failed to mark as read');
      
      setNotifications(prev =>
        prev.map(n =>
          n.id === notificationId ? { ...n, is_read: true } : n
        )
      );
      setUnreadCount(prev => Math.max(0, prev - 1));
    } catch (error) {
      console.error('Error marking notification as read:', error);
    }
  };

  const markAllAsRead = async () => {
    const unreadIds = notifications.filter(n => !n.is_read).map(n => n.id);
    for (const id of unreadIds) {
      await markAsRead(id);
    }
  };

  const getNotificationIcon = (type: string) => {
    switch (type) {
      case 'new_listing': return '🏠';
      case 'price_drop': return '💰';
      case 'similar_listing': return '🔍';
      default: return '🔔';
    }
  };

  const getNotificationColor = (type: string) => {
    switch (type) {
      case 'new_listing': return 'bg-blue-50 border-blue-200';
      case 'price_drop': return 'bg-green-50 border-green-200';
      case 'similar_listing': return 'bg-purple-50 border-purple-200';
      default: return 'bg-gray-50';
    }
  };

  return (
    <div className="relative">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="relative p-2 rounded-full hover:bg-gray-100 transition-colors"
        aria-label="Notifications"
      >
        <svg className="w-6 h-6 text-gray-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} 
            d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
        </svg>
        {unreadCount > 0 && (
          <span className="absolute top-0 right-0 bg-red-500 text-white text-xs rounded-full w-5 h-5 flex items-center justify-center animate-pulse">
            {unreadCount > 9 ? '9+' : unreadCount}
          </span>
        )}
      </button>

      {isOpen && (
        <>
          {/* Backdrop */}
          <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
          
          {/* Dropdown */}
          <div className="absolute right-0 mt-2 w-96 bg-white rounded-lg shadow-xl z-50 max-h-[500px] overflow-hidden">
            <div className="p-3 border-b flex justify-between items-center bg-gray-50">
              <h3 className="font-semibold">Notifications</h3>
              {unreadCount > 0 && (
                <button
                  onClick={markAllAsRead}
                  className="text-xs text-blue-600 hover:text-blue-800"
                >
                  Mark all as read
                </button>
              )}
            </div>
            
            <div className="overflow-y-auto max-h-[400px]">
              {notifications.length === 0 ? (
                <div className="p-8 text-center text-gray-500">
                  <span className="text-4xl opacity-50">🔔</span>
                  <p className="mt-2 text-sm">No notifications yet</p>
                </div>
              ) : (
                notifications.map(notification => (
                  <div
                    key={notification.id}
                    className={`p-3 border-b hover:bg-gray-50 cursor-pointer transition ${!notification.is_read ? 'bg-blue-50' : ''
                      } ${getNotificationColor(notification.type)}`}
                    onClick={() => markAsRead(notification.id)}
                  >
                    <div className="flex items-start">
                      <span className="text-2xl mr-3">
                        {getNotificationIcon(notification.type)}
                      </span>
                      <div className="flex-1 min-w-0">
                        <div className="font-semibold text-sm">{notification.title}</div>
                        <div className="text-sm text-gray-600 mt-0.5">
                          {notification.message}
                        </div>
                        {notification.listing_id && (
                          <Link
                            to={`/listing/${notification.listing_id}`}
                            className="text-xs text-blue-600 hover:underline mt-1 inline-block"
                            onClick={(e) => e.stopPropagation()}
                          >
                            View property →
                          </Link>
                        )}
                        <div className="text-xs text-gray-400 mt-1">
                          {new Date(notification.created_at).toLocaleString()}
                        </div>
                      </div>
                      {!notification.is_read && (
                        <div className="w-2 h-2 bg-blue-500 rounded-full mt-2"></div>
                      )}
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        </>
      )}
    </div>
  );
};

export default NotificationBell;