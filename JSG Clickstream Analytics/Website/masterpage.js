/**
 * masterpage.js
 * 
 * This script tracks key user interactions on all pages of the Wix site.
 * Events are sent to a Cloud Run endpoint for analytics (page views, navigation clicks, subscribe button submissions, and donate link clicks).
 * 
 * Notes:
 * - Only one page view per page per session is sent (pageViewSent flag).
 * - Navigation clicks are tracked from the main horizontal menu (#horizontalMenu2).
 * - Subscribe button (#button16) and donate link (#text67) clicks are tracked individually.
 */

import wixLocation from 'wix-location';
import wixWindow from 'wix-window';

// -------------------------------
// Configuration
// -------------------------------
const ENDPOINT_URL = "https://jsg-clickstream-ingest-691849607520.us-west1.run.app";

/**
 * Sends a click/event object to the Cloud Run endpoint.
 * @param {Object} event - Event data containing event_type, page, and other properties
 */
function sendClickEvent(event) {
    fetch(ENDPOINT_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(event)
    }).catch(err => console.error("Event send failed", err));
}

// -------------------------------
// State flags
// -------------------------------
let lastPage = null;
let pageViewSent = false;

// -------------------------------
// Main onReady function
// -------------------------------
$w.onReady(() => {

    // -------------------------------
    // PAGE VIEW
    // -------------------------------
    let currentPage = "/" + wixLocation.path; // homepage will be "/"
    if (!pageViewSent) {
        sendClickEvent({
            event_type: "page_view",
            page: currentPage,
            timestamp: new Date().toISOString()
        });
        pageViewSent = true;
    }

    // -------------------------------
    // NAVIGATION LINK CLICKS
    // Tracks clicks on all items inside #horizontalMenu2
    // -------------------------------
    const menu = $w("#horizontalMenu2");
    if (menu) {
        menu.onItemClick((event) => {
            sendClickEvent({
                event_type: "navigation_click",
                from: currentPage,
                to: event.item.link, // URL of the clicked menu item
                timestamp: new Date().toISOString()
            });
        });
    }

    // -------------------------------
    // SUBSCRIBE BUTTON
    // Tracks clicks on the subscribe form submit button (#button16)
    // -------------------------------
    const subscribeButton = $w("#button16");
    if (subscribeButton) {
        subscribeButton.onClick(() => {
            sendClickEvent({
                event_type: "subscribe_submit",
                page: currentPage,
                timestamp: new Date().toISOString()
            });
        });
    }

    // -------------------------------
    // DONATE LINK CLICK
    // Tracks clicks on the donate link (#text67)
    // -------------------------------
    const donateText = $w("#text67");
    
    if (donateText) {
        donateText.onClick(() => {
            sendClickEvent({
                event_type: "donate_outbound_click",
                page: currentPage,
                destination: "givebutter",
                timestamp: new Date().toISOString()
            });
        });
    }

});


