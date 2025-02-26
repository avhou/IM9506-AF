from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal
from textual.widgets import Static
from textual.reactive import reactive
import sqlite3

# Sample SQLite setup (replace with your actual DB connection)
def create_sample_db():
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE data (id INTEGER PRIMARY KEY, url TEXT, host TEXT, keywords TEXT, content TEXT, decision TEXT)
    """)
    cursor.executemany("""
        INSERT INTO data (url, host, keywords, content, decision)
        VALUES (?, ?, ?, ?, ?)
    """, [
        ("http://example.com", "example.com", "sample, test", "This is some content for row 1.", ""),
        ("http://another.com", "another.com", "demo, check", """This is some content for row 2.""", ""),
    ])
    conn.commit()
    return conn

class DatabaseViewer(App):
    CSS = """
    Screen { align: center middle; }
    .metadata { width: 25%; }
    .content {
        width: 100%;
        height: 97%;  /* Fixed height for content area */
        overflow-y: auto;  /* Scroll if content exceeds space */
        padding: 1;
    }
    """

    index = reactive(0)

    def __init__(self):
        super().__init__()
        self.conn = create_sample_db()
        self.cursor = self.conn.cursor()
        self.cursor.execute("SELECT * FROM data")
        self.rows = self.cursor.fetchall()

    def compose(self) -> ComposeResult:
        """Creates the UI layout."""
        if not self.rows:
            yield Static("No data found.")
            return

        yield Container(
            Horizontal(
                Static("", id="url", classes="metadata"),
                Static("", id="host", classes="metadata"),
                Static("", id="keywords", classes="metadata"),
                Static("", id="decision", classes="metadata"),
            ),
            Static("", id="content", classes="content"),  # Keep content at the top of the layout
            Static("Press 'h' (prev), 'l' (next), 'y/n/m' for decision, 'q' to quit.", id="footer")
        )
        self.call_after_refresh(self.update_display)

    def update_display(self):
        """Updates UI with current row data."""
        if not self.rows:
            return
        row = self.rows[self.index]
        self.query_one("#url", Static).update(f"URL: {row[1]}")
        self.query_one("#host", Static).update(f"Host: {row[2]}")
        self.query_one("#keywords", Static).update(f"Keywords: {row[3]}")
        self.query_one("#content", Static).update(f"Content: {row[4]}")
        self.query_one("#decision", Static).update(f"Decision: {row[5] if row[5] else 'Pending'}")

    def on_key(self, event):
        """Handles key press events."""
        if event.key == "h":  # Previous row
            self.index = max(0, self.index - 1)
        elif event.key == "l":  # Next row
            self.index = min(len(self.rows) - 1, self.index + 1)
        elif event.key in ("y", "n", "m"):  # Decision keys
            decision = event.key
            row_id = self.rows[self.index][0]
            self.rows[self.index] = (*self.rows[self.index][:5], decision)  # Update decision in memory
            self.cursor.execute("UPDATE data SET decision = ? WHERE id = ?", (decision, row_id))
            self.conn.commit()
        elif event.key == "q":
            self.exit()
        self.update_display()

if __name__ == "__main__":
    DatabaseViewer().run()

