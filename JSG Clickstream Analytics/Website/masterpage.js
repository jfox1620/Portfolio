import wixLocation from 'wix-location';
import wixWindow from 'wix-window';
import { session } from 'wix-storage';



const ENDPOINT_URL = "https://jsg-clickstream-ingest-691849607520.us-west1.run.app";

let sessionId = null;

/**
 * Initializes a unique session ID for the user.
 * - If a session ID already exists in session storage, it is reused.
 * - If none exists, a new UUID is generated and stored.
 */
function initSession() {
    sessionId = session.getItem("session_id");

    if (!sessionId) {
        sessionId = crypto.randomUUID();
        session.setItem("session_id", sessionId);
    }
}

/**
 * Sends a clickstream event to the ingestion endpoint.
 * @param {Object} event - An object representing the event payload. 
 *                         Should include keys like event_type, page, session_id, timestamp, etc.
 */
function sendClickEvent(event) {
    fetch(ENDPOINT_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(event)
    }).catch(err => console.error("Event send failed", err));
}

let lastPage = null;
let pageViewSent = false;

$w.onReady(() => {

    // Initialize session before any event fires
    initSession();

    const currentPage = "/" + wixLocation.path;

    /* -------------------------------
       PAGE VIEW
       Sends a page_view event when the user first loads the page
    -------------------------------- */

    if (!pageViewSent) {
        sendClickEvent({
            event_type: "page_view",
            page: currentPage,
            session_id: sessionId,
            timestamp: new Date().toISOString()
        });
        pageViewSent = true;
    }

    /* -------------------------------
       NAVIGATION LINK CLICKS
       Captures clicks on all links within the horizontal menu (#horizontalMenu2)
    -------------------------------- */
	const menu = $w("#horizontalMenu2");

	if (menu) {
		menu.onItemClick((event) => {
			sendClickEvent({
				event_type: "navigation_click",
                session_id: sessionId,
				from_page: currentPage,
				to_page: event.item.link, // URL of the clicked menu item
				timestamp: new Date().toISOString()
			});
		});
	}

    /* -------------------------------
       SUBSCRIBE FORM SUBMIT
       Captures clicks on the subscribe form submit button (#button16)
    -------------------------------- */
    const subscribeButton = $w("#button16");

	if (subscribeButton) {
		subscribeButton.onClick(() => {
			sendClickEvent({
				event_type: "subscribe_submit",
				page: currentPage,
                session_id: sessionId,
				timestamp: new Date().toISOString()
			});
		});
	}

});
