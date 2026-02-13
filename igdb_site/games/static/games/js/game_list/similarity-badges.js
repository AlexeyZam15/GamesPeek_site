// games/static/games/js/game_list/similarity-badges.js
const SimilarityBadges = {
    init: function() {
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this.initBadges());
        } else {
            this.initBadges();
        }
        document.addEventListener('games-grid-updated', () => {
            setTimeout(() => this.initBadges(), 50);
        });
    },

    initBadges: function() {
        const isSimilarMode = document.querySelector('.similarity-mode-indicator') !== null ||
                             window.location.search.includes('find_similar=1');

        if (!isSimilarMode) return;

        // ТОЛЬКО бейджи схожести, НЕ ТРОГАЕМ кнопки Compare
        const placeholders = document.querySelectorAll('.similarity-badge-placeholder');

        placeholders.forEach(placeholder => {
            const gameId = placeholder.dataset.gameId;
            let similarity = placeholder.dataset.similarity;

            if (!similarity && window.similarityMap && window.similarityMap[gameId]) {
                similarity = window.similarityMap[gameId];
            }

            if (similarity && parseFloat(similarity) > 0) {
                this.renderBadge(placeholder, similarity);
            }
        });
    },

    renderBadge: function(placeholder, similarity) {
        const size = 60;
        const similarityValue = parseFloat(similarity);
        const similarityText = `${Math.round(similarityValue)}%`;

        let color = '#3b82f6';
        if (similarityValue >= 80) color = '#10b981';
        else if (similarityValue >= 60) color = '#f59e0b';

        const svg = `
        <svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 60 60">
            <circle cx="30" cy="30" r="20" fill="${color}" fill-opacity="0.9"/>
            <path d="M30 3.75L33 16.25L43.75 18L36.25 25.5L38.75 36.25L30 31.25L21.25 36.25L23.75 25.5L16.25 18L27 16.25Z"
                  fill="${color}" fill-opacity="0.8" transform="rotate(0,30,30)"/>
            <path d="M30 3.75L33 16.25L43.75 18L36.25 25.5L38.75 36.25L30 31.25L21.25 36.25L23.75 25.5L16.25 18L27 16.25Z"
                  fill="${color}" fill-opacity="0.8" transform="rotate(45,30,30)"/>
            <path d="M30 3.75L33 16.25L43.75 18L36.25 25.5L38.75 36.25L30 31.25L21.25 36.25L23.75 25.5L16.25 18L27 16.25Z"
                  fill="${color}" fill-opacity="0.8" transform="rotate(90,30,30)"/>
            <path d="M30 3.75L33 16.25L43.75 18L36.25 25.5L38.75 36.25L30 31.25L21.25 36.25L23.75 25.5L16.25 18L27 16.25Z"
                  fill="${color}" fill-opacity="0.8" transform="rotate(135,30,30)"/>
            <text x="30" y="35" text-anchor="middle" fill="white" font-family="Arial, sans-serif" font-weight="bold" font-size="12">${similarityText}</text>
        </svg>
        `;

        const svgEncoded = btoa(unescape(encodeURIComponent(svg)));
        placeholder.innerHTML = `<img src="data:image/svg+xml;base64,${svgEncoded}" width="${size}" height="${size}" alt="Similarity: ${similarityText}" title="Similarity: ${similarityText}" class="similarity-svg">`;
        placeholder.style.display = 'block';
    },

    refresh: function() {
        this.initBadges();
    }
};

SimilarityBadges.init();
window.SimilarityBadges = SimilarityBadges;