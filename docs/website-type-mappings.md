# Website Type Mappings Reference

This document provides reference information for adding new place types to the Charlotte Third Places website.

## Adding a New Type: Comic Book Store

The following changes should be applied to the [charlotte-third-places](https://github.com/segunak/charlotte-third-places) repository.

### 1. Icons.tsx Changes

**File:** `charlotte-third-places/components/Icons.tsx`

#### Add Import

Add `GiDominoMask` to the existing `react-icons/gi` import (keep all existing imports):

```tsx
// Note: This shows only the gi imports - keep all other existing imports in the file
import {
  GiPlantSeed,
  GiCoffeeMug,
  GiDominoMask  // Add this to the existing imports
} from "react-icons/gi";
```

#### Update typeIconMap

Add the Comic Book Store entry to `typeIconMap`:

```tsx
export const typeIconMap: { [key: string]: React.ComponentType<any> } = {
  "Bakery": FaBreadSlice,
  "Bottle Shop": FaWineBottle,
  "Café": GiCoffeeMug,
  "Coffee Shop": FaCoffee,
  "Tea House": MdEmojiFoodBeverage,
  "Bubble Tea Shop": RiDrinks2Fill,
  "Restaurant": FaUtensils,
  "Market": FaStore,
  "Grocery Store": FaShoppingCart,
  "Library": FaBook,
  "Bookstore": FaBookOpen,
  "Game Store": FaGamepad,
  "Garden": GiPlantSeed,
  "Brewery": FaBeer,
  "Deli": IoFastFood,
  "Eatery": FaUtensils,
  "Creamery": FaIceCream,
  "Ice Cream Shop": FaIceCream,
  "Art Gallery": FaPalette,
  "Bar": FaCocktail,
  "Community Center": FaUsers,
  "Coworking Space": FaLaptop,
  "Museum": FaUniversity,
  "Other": FaQuestion,
  "Photo Shop": IoCamera,
  "Lounge": FaCouch,
  "Comic Book Store": GiDominoMask,  // Add this line
};
```

### 2. PlaceCard.tsx Changes

**File:** `charlotte-third-places/components/PlaceCard.tsx`

#### Update typeEmojiMap

Add the Comic Book Store entry to `typeEmojiMap`:

```tsx
const typeEmojiMap: { [key: string]: string } = {
    "Bakery": "🍞",
    "Bottle Shop": "🍷",
    "Café": "☕",
    "Coffee Shop": "☕",
    "Tea House": "🍵",
    "Bubble Tea Shop": "🧋",
    "Restaurant": "🍽️",
    "Market": "🛍️",
    "Grocery Store": "🛒",
    "Market Hall": "🏬",
    "Library": "📚",
    "Bookstore": "📖",
    "Public Market": "🏪",
    "Game Store": "🎮",
    "Garden": "🪴",
    "Brewery": "🍺",
    "Deli": "🥪",
    "Eatery": "🍴",
    "Creamery": "🍦",
    "Ice Cream Shop": "🍨",
    "Art Gallery": "🖼️",
    "Bar": "🍸",
    "Community Center": "🤝",
    "Coworking Space": "💻",
    "Lounge": "🛋️",
    "Museum": "🏛️",
    "Other": "🤷🏾",
    "Photo Shop": "📷",
    "Comic Book Store": "🦸",  // Add this line
};
```

## Design Rationale

### Icon: GiDominoMask
- The domino mask is the classic superhero mask worn by iconic comic book characters
- Immediately evokes comic book heroes and the superhero genre
- Available in the `react-icons/gi` (Game Icons) package which is already used in the project

### Emoji: 🦸 (Superhero)
- Universally recognized superhero emoji
- Directly represents the core content sold at comic book stores
- Matches the mask icon thematically

## Alternative Options

If different icon/emoji choices are preferred (all icons verified to exist in `react-icons`):

| Type | Icon Option | Emoji Option | Notes |
|------|-------------|--------------|-------|
| Superhero Theme | `GiDominoMask` | 🦸 | **Recommended** - Strong comic association |
| Batman Theme | `GiBatMask` | 🦹 | More specific to Batman/dark heroes |
| Book Theme | `FaBookOpen` | 📖 | Generic bookstore feel, less comic-specific |
| Speech Bubble | `GiChatBubble` | 💬 | Represents comic panels/dialogue |

> **Note:** Verify icon availability in the [React Icons documentation](https://react-icons.github.io/react-icons/) before implementing alternatives.
