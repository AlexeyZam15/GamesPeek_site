// games/static/games/js/game_list/similarity-display.js

const SimilarityDisplay = {
    init: function() {
        console.log('SimilarityDisplay initializing...');

        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this.displayAllSimilarities());
        } else {
            this.displayAllSimilarities();
        }

        // Слушаем обновление сетки игр
        document.addEventListener('games-grid-updated', () => {
            console.log('Games grid updated, updating similarities...');
            setTimeout(() => this.displayAllSimilarities(), 50);
        });

        // Наблюдаем за DOM изменениями
        const observer = new MutationObserver((mutations) => {
            let shouldUpdate = false;
            mutations.forEach(mutation => {
                if (mutation.addedNodes.length) {
                    mutation.addedNodes.forEach(node => {
                        if (node.nodeType === 1) {
                            if (node.classList && node.classList.contains('game-card-container')) {
                                shouldUpdate = true;
                            }
                            if (node.querySelector && node.querySelector('.game-card-container')) {
                                shouldUpdate = true;
                            }
                        }
                    });
                }
            });
            if (shouldUpdate) {
                setTimeout(() => this.displayAllSimilarities(), 50);
            }
        });

        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
    },

    displayAllSimilarities: function() {
        const containers = document.querySelectorAll('.game-card-container[data-similarity]');
        console.log(`Found ${containers.length} cards with similarity data`);

        containers.forEach(container => {
            this.displaySimilarityForCard(container);
        });
    },

    displaySimilarityForCard: function(container) {
        const similarity = parseFloat(container.dataset.similarity);
        if (isNaN(similarity)) return;

        // Ищем контейнер для процента
        let similarityContainer = container.querySelector('.similarity-container');

        // Если нет контейнера, создаем его
        if (!similarityContainer) {
            const imgContainer = container.querySelector('.card-img-top-container');
            if (!imgContainer) return;

            similarityContainer = document.createElement('div');
            similarityContainer.className = 'position-absolute top-0 end-0 p-1 similarity-container';
            imgContainer.appendChild(similarityContainer);
        }

        // Очищаем контейнер
        similarityContainer.innerHTML = '';

        // Создаем SVG с процентом (точная копия тега similarity_pattern_svg)
        const svg = this.createSimilaritySVG(similarity, 60);
        similarityContainer.appendChild(svg);

        console.log(`Displayed ${similarity}% for game ${container.dataset.gameId}`);
    },

    getSimilarityColor: function(percent) {
        // Точная копия функции get_similarity_color из color_tags.py
        if (percent >= 80) {
            return '#10b981';  // зеленый
        } else if (percent >= 60) {
            return '#f59e0b';  // желтый/оранжевый
        } else {
            return '#3b82f6';  // синий
        }
    },

    createSimilaritySVG: function(percent, size) {
        // Округляем процент
        const roundedPercent = Math.round(percent);
        const similarityText = roundedPercent + '%';
        const color = this.getSimilarityColor(roundedPercent);

        // Рассчитываем размеры пропорционально
        const font_size = Math.max(9, 12 * size / 60);
        const circle_radius = Math.max(15, 20 * size / 60);

        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('width', size);
        svg.setAttribute('height', size);
        svg.setAttribute('viewBox', '0 0 60 60');

        // Центральный круг
        const centerCircle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        centerCircle.setAttribute('cx', '30');
        centerCircle.setAttribute('cy', '30');
        centerCircle.setAttribute('r', circle_radius);
        centerCircle.setAttribute('fill', color);
        centerCircle.setAttribute('fill-opacity', '0.9');
        svg.appendChild(centerCircle);

        // Внешние выступы (4 штуки с поворотом на 0, 45, 90, 135 градусов)
        const createOuterShape = (rotate) => {
            const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            path.setAttribute('d', 'M30 3.75L33 16.25L43.75 18L36.25 25.5L38.75 36.25L30 31.25L21.25 36.25L23.75 25.5L16.25 18L27 16.25Z');
            path.setAttribute('fill', color);
            path.setAttribute('fill-opacity', '0.8');
            path.setAttribute('transform', `rotate(${rotate},30,30)`);
            return path;
        };

        svg.appendChild(createOuterShape(0));
        svg.appendChild(createOuterShape(45));
        svg.appendChild(createOuterShape(90));
        svg.appendChild(createOuterShape(135));

        // Текст процента
        const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', '30');
        text.setAttribute('y', '35');
        text.setAttribute('text-anchor', 'middle');
        text.setAttribute('fill', 'white');
        text.setAttribute('font-family', 'Arial, sans-serif');
        text.setAttribute('font-weight', 'bold');
        text.setAttribute('font-size', font_size);
        text.setAttribute('style', 'text-shadow: 1px 1px 2px rgba(0,0,0,0.8);');
        text.textContent = similarityText;
        svg.appendChild(text);

        // Создаем img тэг с base64 для точного соответствия оригиналу
        const svgString = new XMLSerializer().serializeToString(svg);
        const base64 = btoa(svgString);

        const img = document.createElement('img');
        img.src = 'data:image/svg+xml;base64,' + base64;
        img.width = size;
        img.height = size;
        img.alt = 'Similarity: ' + similarityText;
        img.title = 'Similarity';
        img.className = 'similarity-svg';

        return img;
    },

    refresh: function() {
        this.displayAllSimilarities();
    }
};

// Инициализация
SimilarityDisplay.init();

// Делаем доступным глобально
window.SimilarityDisplay = SimilarityDisplay;