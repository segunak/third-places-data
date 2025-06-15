class PhotoViewer {
    constructor() {
        this.places = [];
        this.currentPlace = null;
        this.isSelectMode = false;
        this.selectedPhotos = new Set();
        this.initializeElements();
        this.bindEvents();
        this.loadPlaces(); // Call loadPlaces, which will use the API
    }

    initializeElements() {
        this.placeSelect = document.getElementById('placeSelect');
        this.selectModeBtn = document.getElementById('selectModeBtn');
        this.downloadSelectedBtn = document.getElementById('downloadSelectedBtn');
        this.selectedCount = document.getElementById('selectedCount');
        this.welcomeMessage = document.getElementById('welcomeMessage');
        this.loadingMessage = document.getElementById('loadingMessage');
        this.photoSections = document.getElementById('photoSections');
        this.photoUrlsGrid = document.getElementById('photoUrlsGrid');
        this.rawDataGrid = document.getElementById('rawDataGrid');
        this.photoUrlsSection = document.getElementById('photoUrlsSection');
        this.rawDataSection = document.getElementById('rawDataSection');
        this.modal = document.getElementById('photoModal');
        this.modalImage = document.getElementById('modalImage');
        this.modalMetadata = document.getElementById('modalMetadata');
        this.modalDownloadBtn = document.getElementById('modalDownloadBtn');
        this.errorToast = document.getElementById('errorToast');
    }

    bindEvents() {
        this.placeSelect.addEventListener('change', (e) => this.onPlaceChange(e.target.value));
        this.selectModeBtn.addEventListener('click', () => this.toggleSelectMode());
        this.downloadSelectedBtn.addEventListener('click', () => this.downloadSelected());

        // Modal events
        this.modal.querySelector('.close').addEventListener('click', () => this.closeModal());
        this.modal.addEventListener('click', (e) => {
            if (e.target === this.modal) this.closeModal();
        });
        this.modalDownloadBtn.addEventListener('click', () => this.downloadModalImage());

        // Keyboard events
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') this.closeModal();
        });
    }

    async loadPlaces() {
        try {
            // Attempt to load from API first
            await this.loadPlacesFromAPI();
        } catch (error) {
            console.error('Error loading places from API:', error);
            this.showError('Could not load place list from server. Please ensure serve.py is running and check console for errors.');
        }
    }

    async loadPlacesFromAPI() {
        const response = await fetch('/api/list-places');
        if (!response.ok) {
            throw new Error(`API request failed: ${response.status} ${response.statusText}`);
        }
        const files = await response.json();
        if (!files || files.length === 0) {
            this.showError('No place data files found by the server. Check the /api/list-places endpoint.');
            return;
        }

        const places = [];
        for (const filename of files) {
            const fileUrl = `/places/charlotte/${filename}`;
            try {
                const fileResponse = await fetch(fileUrl);
                if (fileResponse.ok) {
                    const data = await fileResponse.json();
                    const placeName = this.extractPlaceName(data, filename);
                    places.push({
                        filename: filename,
                        name: placeName,
                        data: data
                    });
                } else {
                    this.showError(`Could not load place data: ${filename} (${fileResponse.status})`);
                }
            } catch (error) {
                this.showError(`Error loading data for: ${filename} - ${error.message}`);
            }
        }

        if (places.length === 0) {
            this.showError('No valid place files could be loaded. Check server logs and file contents.');
            return;
        }

        this.places = places.sort((a, b) => a.name.localeCompare(b.name));
        this.populatePlaceSelect();
    }

    extractPlaceName(data, filenameOrUrl) {
        if (data.place_name) return data.place_name;
        if (data.details && data.details.place_name) return data.details.place_name;
        if (data.details && data.details.raw_data && data.details.raw_data.name) return data.details.raw_data.name;
        if (data.name) return data.name;
        if (data.title) return data.title;
        if (data.business_name) return data.business_name;

        const fileName = filenameOrUrl.split('/').pop() || filenameOrUrl;
        return fileName.replace('.json', '');
    }

    populatePlaceSelect() {
        this.placeSelect.innerHTML = '<option value="">Select a place...</option>';

        this.places.forEach((place, index) => {
            const option = document.createElement('option');
            option.value = index;
            option.textContent = place.name;
            this.placeSelect.appendChild(option);
        });
    }

    async onPlaceChange(selectedIndex) {
        if (!selectedIndex) {
            this.showWelcomeMessage();
            return;
        }

        const place = this.places[selectedIndex];
        this.currentPlace = place;
        await this.loadPlacePhotos(place);
    }

    async loadPlacePhotos(place) {
        this.showLoadingMessage();

        try {
            const photos = this.extractPhotos(place.data);
            this.displayPhotos(photos);
            this.showPhotoSections();
        } catch (error) {
            console.error('Error loading photos:', error);
            this.showError('Error loading photos for this place.');
        }
    } extractPhotos(data) {
        const photos = {
            photoUrls: [],
            rawData: []
        };

        // Extract from photos.photo_urls if it exists
        if (data.photos && data.photos.photo_urls && Array.isArray(data.photos.photo_urls)) {
            photos.photoUrls = data.photos.photo_urls.map((url, index) => ({
                id: `url_${index}`,
                url: url,
                bigUrl: url,
                type: 'photo_url',
                index: index
            }));
        }

        // Extract from photos.raw_data if it exists and is an array
        if (data.photos && data.photos.raw_data && Array.isArray(data.photos.raw_data)) {
            photos.rawData = data.photos.raw_data.map((item, index) => ({
                id: `raw_${index}`,
                url: item.photo_url || item.photo_url_big,
                bigUrl: item.photo_url_big || item.photo_url,
                type: 'raw_data',
                index: index,
                metadata: {
                    photo_id: item.photo_id,
                    date: item.photo_date,
                    source: item.photo_upload_source,
                    tags: item.photo_tags,
                    latitude: item.latitude,
                    longitude: item.longitude
                }
            }));
        }

        return photos;
    }

    displayPhotos(photos) {
        this.clearSelectedPhotos();
        this.photoUrlsGrid.innerHTML = '';
        this.rawDataGrid.innerHTML = '';

        if (photos.photoUrls.length > 0) {
            this.photoUrlsSection.style.display = 'block';
            photos.photoUrls.forEach(photo => {
                this.photoUrlsGrid.appendChild(this.createPhotoElement(photo));
            });
        } else {
            this.photoUrlsSection.style.display = 'none';
        }

        if (photos.rawData.length > 0) {
            this.rawDataSection.style.display = 'block';
            photos.rawData.forEach(photo => {
                this.rawDataGrid.appendChild(this.createPhotoElement(photo));
            });
        } else {
            this.rawDataSection.style.display = 'none';
        }

        if (photos.photoUrls.length === 0 && photos.rawData.length === 0) {
            this.showError('No photos found for this place.');
        }
    } createPhotoElement(photo) {
        const photoItem = document.createElement('div');
        photoItem.className = 'photo-item';
        photoItem.dataset.photoId = photo.id;

        const photoContainer = document.createElement('div');
        photoContainer.className = 'photo-container';

        if (this.isSelectMode) {
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'photo-checkbox';
            checkbox.addEventListener('change', (e) => this.onPhotoSelect(photo.id, e.target.checked));
            photoContainer.appendChild(checkbox);
            photoItem.classList.add('select-mode');
        }

        // Check if this is a GPS URL that might be blocked
        const isGpsUrl = photo.url && photo.url.includes('/gps');

        if (isGpsUrl) {
            // For GPS URLs, show a warning and try alternative methods
            this.createGpsWarningElement(photoContainer, photo);
        } else {
            // Normal image loading for non-GPS URLs
            const img = document.createElement('img');
            img.src = photo.url;
            img.alt = `Photo ${photo.index + 1}`;
            img.loading = 'lazy';

            // Try to work around CORS issues
            img.crossOrigin = 'anonymous';
            img.referrerPolicy = 'no-referrer';

            img.addEventListener('load', () => {
                // Image loaded successfully - no additional action needed
            });

            img.addEventListener('error', () => {
                this.createErrorElement(photoContainer, photo.url);
            });

            img.addEventListener('click', () => {
                if (!this.isSelectMode) {
                    this.openModal(photo);
                }
            });

            // Add the image to container immediately for better UX
            photoContainer.appendChild(img);
        }

        photoItem.appendChild(photoContainer);

        const photoInfo = document.createElement('div');
        photoInfo.className = 'photo-info';

        if (photo.metadata) {
            const metadata = document.createElement('div');
            metadata.className = 'photo-metadata';

            if (photo.metadata.date) {
                const dateDiv = document.createElement('div');
                dateDiv.textContent = `Date: ${photo.metadata.date}`;
                metadata.appendChild(dateDiv);
            }

            if (photo.metadata.source) {
                const sourceDiv = document.createElement('div');
                sourceDiv.textContent = `Source: ${photo.metadata.source}`;
                metadata.appendChild(sourceDiv);
            }

            if (photo.metadata.latitude && photo.metadata.longitude) {
                const locationDiv = document.createElement('div');
                locationDiv.textContent = `Location: ${photo.metadata.latitude}, ${photo.metadata.longitude}`;
                metadata.appendChild(locationDiv);
            }

            photoInfo.appendChild(metadata);

            if (photo.metadata.tags && photo.metadata.tags.length > 0) {
                const tagsContainer = document.createElement('div');
                tagsContainer.className = 'photo-tags';

                photo.metadata.tags.forEach(tag => {
                    const tagSpan = document.createElement('span');
                    tagSpan.className = 'photo-tag';
                    tagSpan.textContent = tag;
                    tagsContainer.appendChild(tagSpan);
                });

                photoInfo.appendChild(tagsContainer);
            }
        }

        const actions = document.createElement('div');
        actions.className = 'photo-actions';

        const downloadBtn = document.createElement('button');
        downloadBtn.className = 'btn btn-primary btn-small';
        downloadBtn.textContent = 'Download';
        downloadBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            this.downloadPhoto(photo);
        });

        actions.appendChild(downloadBtn);
        photoInfo.appendChild(actions);
        photoItem.appendChild(photoInfo);

        return photoItem;
    }

    createGpsWarningElement(container, photo) {
        const warningDiv = document.createElement('div');
        warningDiv.className = 'photo-gps-warning';

        const icon = document.createElement('div');
        icon.className = 'warning-icon';
        icon.textContent = '🔒';

        const text = document.createElement('div');
        text.className = 'warning-text';
        text.textContent = 'GPS Photo Protected';

        const explanation = document.createElement('div');
        explanation.className = 'warning-explanation';
        explanation.textContent = 'Google Photos with GPS coordinates are protected from external access';

        const urlDiv = document.createElement('div');
        urlDiv.className = 'warning-url';
        urlDiv.textContent = photo.url;

        // Add a button to try opening in new tab
        const openBtn = document.createElement('button');
        openBtn.className = 'btn btn-secondary btn-small';
        openBtn.textContent = 'Open in New Tab';
        openBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            window.open(photo.url, '_blank');
        });

        warningDiv.appendChild(icon);
        warningDiv.appendChild(text);
        warningDiv.appendChild(explanation);
        warningDiv.appendChild(urlDiv);
        warningDiv.appendChild(openBtn);

        container.appendChild(warningDiv);
    }

    createErrorElement(container, url) {
        const errorDiv = document.createElement('div');
        errorDiv.className = 'photo-error';

        const icon = document.createElement('div');
        icon.className = 'error-icon';
        icon.textContent = '⚠️';

        const text = document.createElement('div');
        text.textContent = 'Unable to load image';

        const urlDiv = document.createElement('div');
        urlDiv.className = 'error-url';
        urlDiv.textContent = url;

        errorDiv.appendChild(icon);
        errorDiv.appendChild(text);
        errorDiv.appendChild(urlDiv);
        container.appendChild(errorDiv);
    }

    toggleSelectMode() {
        this.isSelectMode = !this.isSelectMode;
        this.selectModeBtn.textContent = this.isSelectMode ? 'Exit Select Mode' : 'Multi-Select Mode';
        this.selectModeBtn.className = this.isSelectMode ? 'btn btn-danger' : 'btn btn-secondary';

        if (!this.isSelectMode) {
            this.clearSelectedPhotos();
        }

        if (this.currentPlace) {
            this.loadPlacePhotos(this.currentPlace);
        }
    }

    onPhotoSelect(photoId, selected) {
        if (selected) {
            this.selectedPhotos.add(photoId);
        } else {
            this.selectedPhotos.delete(photoId);
        }

        this.updateSelectedCount();
        this.updatePhotoSelection(photoId, selected);
    }

    updatePhotoSelection(photoId, selected) {
        const photoItem = document.querySelector(`[data-photo-id="${photoId}"]`);
        if (photoItem) {
            photoItem.classList.toggle('selected', selected);
        }
    }

    updateSelectedCount() {
        const count = this.selectedPhotos.size;
        this.selectedCount.textContent = `${count} selected`;
        this.downloadSelectedBtn.disabled = count === 0;
    }

    clearSelectedPhotos() {
        this.selectedPhotos.clear();
        this.updateSelectedCount();
    }

    async downloadSelected() {
        if (this.selectedPhotos.size === 0) return;

        const photos = this.getAllCurrentPhotos();
        const selectedPhotoObjects = photos.filter(photo => this.selectedPhotos.has(photo.id));

        for (const photo of selectedPhotoObjects) {
            await this.downloadPhoto(photo);
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }

    getAllCurrentPhotos() {
        if (!this.currentPlace) return [];

        const photos = this.extractPhotos(this.currentPlace.data);
        return [...photos.photoUrls, ...photos.rawData];
    }

    async downloadPhoto(photo) {
        try {
            const url = photo.bigUrl || photo.url;
            const response = await fetch(url);

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const blob = await response.blob();
            const downloadUrl = window.URL.createObjectURL(blob);

            const a = document.createElement('a');
            a.href = downloadUrl;
            a.download = this.generateFilename(photo);
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);

            window.URL.revokeObjectURL(downloadUrl);

        } catch (error) {
            console.error('Download failed:', error);
            this.showError(`Failed to download photo: ${error.message}`);
        }
    }

    generateFilename(photo) {
        const place = this.currentPlace ? this.currentPlace.name.replace(/[^a-zA-Z0-9]/g, '_') : 'unknown';
        const type = photo.type;
        const index = photo.index + 1;
        const extension = 'jpg';

        return `${place}_${type}_${index}.${extension}`;
    }

    openModal(photo) {
        this.modalImage.src = photo.bigUrl || photo.url;
        this.modalImage.alt = `Photo ${photo.index + 1}`;

        this.modalMetadata.innerHTML = '';

        if (photo.metadata) {
            const metadataHtml = [];

            if (photo.metadata.photo_id) {
                metadataHtml.push(`<div><strong>Photo ID:</strong> ${photo.metadata.photo_id}</div>`);
            }
            if (photo.metadata.date) {
                metadataHtml.push(`<div><strong>Date:</strong> ${photo.metadata.date}</div>`);
            }
            if (photo.metadata.source) {
                metadataHtml.push(`<div><strong>Source:</strong> ${photo.metadata.source}</div>`);
            }
            if (photo.metadata.latitude && photo.metadata.longitude) {
                metadataHtml.push(`<div><strong>Location:</strong> ${photo.metadata.latitude}, ${photo.metadata.longitude}</div>`);
            }
            if (photo.metadata.tags && photo.metadata.tags.length > 0) {
                metadataHtml.push(`<div><strong>Tags:</strong> ${photo.metadata.tags.join(', ')}</div>`);
            }

            this.modalMetadata.innerHTML = metadataHtml.join('');
        }

        this.modalDownloadBtn.onclick = () => this.downloadPhoto(photo);

        this.modal.style.display = 'block';
        document.body.style.overflow = 'hidden';
    }

    closeModal() {
        this.modal.style.display = 'none';
        document.body.style.overflow = 'auto';
    }

    downloadModalImage() {
        // This will be handled by the onclick event set in openModal
    }

    showWelcomeMessage() {
        this.welcomeMessage.style.display = 'block';
        this.loadingMessage.style.display = 'none';
        this.photoSections.style.display = 'none';
    }

    showLoadingMessage() {
        this.welcomeMessage.style.display = 'none';
        this.loadingMessage.style.display = 'block';
        this.photoSections.style.display = 'none';
    }

    showPhotoSections() {
        this.welcomeMessage.style.display = 'none';
        this.loadingMessage.style.display = 'none';
        this.photoSections.style.display = 'block';
    }

    showError(message) {
        this.errorToast.textContent = message;
        this.errorToast.classList.add('show');

        setTimeout(() => {
            this.errorToast.classList.remove('show');
        }, 5000);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new PhotoViewer();
});
