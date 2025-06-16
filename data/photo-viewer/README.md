# Photo Viewer - Local Usage Instructions

This photo viewer is designed for local use, allowing you to browse photos from JSON files on your computer without needing a server.

## How to Use

1. **Open the site:**
   - Open `index.html` in your browser (double-click or right-click > Open With > your browser).

2. **Edit the list of photo data files:**
   - Open `js/app.js` in a text editor.
   - At the top, you'll see a section like this:

```js
const PLACE_JSON_FILES = [
    'file:///C:/GitHub/third-places-data/places/charlotte/ChIJ___PjW2fVogRpQSJQZr2tKM.json',
    // ...add more as needed
];
```

   Add or remove `file:///` URLs to your local JSON files as needed. You can drag a JSON file into your browser to get its full file:/// URL.

3. **Save and reload:**
   - Save your changes to `app.js` and reload `index.html` in your browser.

## Notes

- This site does **not** require a server. All data is loaded directly from your local files.
- If you see errors about loading files, check that the file paths are correct and that your browser allows local file access.
- Some browsers (like Chrome) may restrict local file access for security. Firefox is recommended for best compatibility.

## Troubleshooting

- If images or data do not load, double-check the file URLs and file permissions.
- If you want to use a server, you can still use `photo-server.py` or similar, but it is not required for local viewing.

---

For questions or help, see the comments in `js/app.js` or contact the project maintainer.
