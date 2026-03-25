# MeasFlow GitHub Pages

This directory contains the static website for MeasFlow, deployed to GitHub Pages.

## Setup Instructions

To enable GitHub Pages for this repository:

1. Go to **Repository Settings** → **Pages**
2. Under **Source**, select **GitHub Actions** (NOT "Deploy from a branch")
3. The workflow in `.github/workflows/pages.yml` will automatically deploy changes when pushed to `main`

## Files

- `index.html` - Main homepage with comprehensive API documentation
- `styles.css` - Styling for the homepage
- `.nojekyll` - Tells GitHub Pages to skip Jekyll processing (required for static HTML)

## Local Development

To preview the site locally, simply open `index.html` in a web browser, or use a local HTTP server:

```bash
# Using Python
python -m http.server 8000

# Using Node.js
npx serve

# Then open http://localhost:8000
```

## Notes

- The site is pure HTML/CSS/JavaScript with no build process required
- All content is self-contained in the `docs` directory
- The `.nojekyll` file prevents Jekyll from processing the files
