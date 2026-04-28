# 🎯 Updated Chatbot - User-Friendly Listing Links

## What's New

The chatbot now displays **clickable links** to listings when you search, making it super easy to view property details!

## How to Use

### 1. **Search for Listings**
Simply type a natural language query:

```
Examples:
- "Show me apartments in Tunis"
- "Find 3-bedroom houses under 500,000 TND in Sfax"
- "Villas with 4 rooms in Ariana"
- "Commercial spaces in La Marsa"
```

### 2. **View Clickable Links**
The chatbot will respond with:
- ✅ Property details (price, size, rooms)
- ✅ **Clickable "View listing →" links** (blue underlined)
- ✅ Property type emoji (🏢 apartment, 🏡 house, etc.)

### 3. **Click to Open**
Just click the **"View listing →"** link to:
- Open the property details page
- See full images
- Check location on map
- View agent contact info

## Example Conversation

```
You: "Show me 2-bedroom apartments in Tunis under 200,000 TND"

Bot: 🏠 Found 3 listings:

1. 🏢 Modern Apartment in Tunis Center
   💰 180,000 TND • Tunis
   📐 120m² • 2 bed
   🔗 View listing → (CLICKABLE LINK)

2. 🏢 Apartment with Balcony
   💰 165,000 TND • Tunis
   📐 95m² • 2 bed
   🔗 View listing → (CLICKABLE LINK)

3. 🏢 Cozy Apartment
   💰 150,000 TND • Tunis
   📐 85m² • 2 bed
   🔗 View listing → (CLICKABLE LINK)
```

## Features

✅ **Markdown formatting** - Bold text and links render beautifully
✅ **Direct links** - Opens listing details in a new tab
✅ **Multiple search options** - Filter by price, location, rooms, type, etc.
✅ **Real-time streaming** - See results appear word-by-word
✅ **Mobile-friendly** - Works on phones and tablets

## Try These Commands

- "Show me all apartments in Tunis"
- "Find luxury villas with gardens in Sfax"
- "3+ bedroom houses under 1 million TND"
- "Commercial spaces for rent in downtown Tunis"
- "Land available in Skhira"
- "Show me more" (after initial results)

## Technical Details

### Backend Changes
- Search tool now includes listing IDs and URLs
- Agent formats results with markdown links: `[View listing →](url)`
- URLs automatically constructed as `https://estatemind.vercel.app/listing/{id}`

### Frontend Changes
- New markdown parser (`utils/markdown.ts`) handles:
  - **Bold text** (`**text**`)
  - *Italic text* (`*text*`)
  - [Clickable links](url) (`[text](url)`)
- Markdown content renders with styled links:
  - Blue color with bottom border
  - Hover effect: light blue background
  - Opens in new tab with `target="_blank"`

### Styling
- Links have visual feedback (hover states)
- Responsive design works on all screen sizes
- Matches EstateMind's color scheme (green/blue)

---

**Ready to search?** Open the chat widget (💬 button) and start exploring listings! 🏠
