import ssl
import socket
import datetime
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import json

# Discord Webhook URL (replace with your valid URL)
DISCORD_WEBHOOK_URL = ""

def get_cert_expiry_date(domain):
    """Get the certificate expiry date of a domain"""
    context = ssl.create_default_context()
    try:
        with socket.create_connection((domain, 443), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=domain) as ssock:
                cert = ssock.getpeercert()
                expiry_date = datetime.datetime.strptime(cert['notAfter'], '%b %d %H:%M:%S %Y %Z')
                return expiry_date
    except Exception as e:
        print(f"Error checking {domain}: {e}")
        return None

def send_discord_notification(message):
    """Send notification to Discord"""
    # Ensure data format meets Discord Webhook requirements
    data = {
        "content": message,
        "username": "SSL Certificate Monitor"
    }
    json_data = json.dumps(data).encode('utf-8')

    # Create request
    req = Request(
        DISCORD_WEBHOOK_URL,
        data=json_data,
        headers={
            'Content-Type': 'application/json',
            'User-Agent': 'SSL-Monitor/1.0'  # Add User-Agent to avoid being flagged as suspicious
        },
        method='POST'
    )

    try:
        with urlopen(req, timeout=10) as response:
            print(f"Response status code: {response.status}")
            print(f"Response content: {response.read().decode('utf-8')}")
            if response.status == 204:
                print("Notification sent successfully")
            else:
                print(f"Failed to send Discord notification, status code: {response.status}")
    except HTTPError as e:
        print(f"Error sending Discord notification: {e}")
        print(f"Error response content: {e.read().decode('utf-8')}")
    except URLError as e:
        print(f"Network error: {e}")
    except Exception as e:
        print(f"Other error: {e}")

def check_domains(domains):
    """Check domain list and send notifications"""
    current_date = datetime.datetime.now()

    for domain in domains:
        expiry_date = get_cert_expiry_date(domain)
        if expiry_date:
            days_left = (expiry_date - current_date).days

            if days_left < 10:
                message = f"⚠️ Warning: The SSL certificate for {domain} will expire in {days_left} days (Expiry date: {expiry_date.strftime('%Y-%m-%d')})"
                print(message)
                send_discord_notification(message)
            else:
                print(f"The certificate for {domain} has {days_left} days remaining")

# List of domains to check
domains_to_check = [
    "dagon.ckb.guide",
    "reaver.ckb.guide",
    "clarity.ckb.guide",
]

if __name__ == "__main__":
    # Start checking domains
    print("\nStarting domain check...")
    check_domains(domains_to_check)
