import requests
from requests.auth import HTTPBasicAuth
import time
import random
import functools
from datetime import datetime
import argparse
import os
import pandas as pd
import winsound
import json

parser = argparse.ArgumentParser(description='WorldQuant Alpha Submitter')
parser.add_argument('--credentials_file', type=str, default="brain_credentials.txt", help='Credentials file')
parser.add_argument('--start_date', type=str, default="01-01", help='Start date (MM-DD format)')
parser.add_argument('--end_date', type=str, default="12-01", help='End date (MM-DD format)')
parser.add_argument('--alpha_num', type=int, default=10000, help='Number of Alphas to check')
parser.add_argument('--sharpe_th', type=float, default=1.25, help='Sharpe threshold')
parser.add_argument('--fitness_th', type=float, default=1.0, help='Fitness threshold')
parser.add_argument('--turnover_th', type=float, default=0.3, help='Turnover threshold')
parser.add_argument('--submit_delay', type=int, default=70, help='Delay time between submissions (seconds)')
parser.add_argument('--max_submitted_change', type=int, default=2, help='Maximum allowed change in submitted Alpha count')
parser.add_argument('--region', type=str, default="USA", help='Region')
parser.add_argument('--blacklist_file', type=str, default="blacklist.txt", help='Blacklist file path')

args = parser.parse_args()
condition = True  # Sound switch

def read_credentials(file_path):
    username = ""
    password = ""
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                content = file.read().strip()
                try:
                    credentials = json.loads(content)
                    if len(credentials) >= 1:
                        username = credentials[0]
                    if len(credentials) >= 2:
                        password = credentials[1]
                except json.JSONDecodeError:
                    lines = content.split('\n')
                    if len(lines) >= 1:
                        username = lines[0].strip()
                    if len(lines) >= 2:
                        password = lines[1].strip()
            return username, password
        else:
            print(f"Credentials file {file_path} does not exist")
            return "", ""
    except Exception as e:
        print(f"Error reading credentials file: {e}")
        return "", ""

def read_blacklist(file_path):
    blacklist = set()
    try:
        if not os.path.exists(file_path):
            with open(file_path, 'w') as file:
                pass
            print(f"Blacklist file {file_path} does not exist, created new file")
        else:
            with open(file_path, 'r') as file:
                for line in file:
                    blacklist.add(line.strip())
            print(f"Read {len(blacklist)} Alpha IDs from blacklist file")
    except Exception as e:
        print(f"Error reading or creating blacklist file: {e}")
    return blacklist


def update_blacklist(file_path, alpha_id):
    try:
        with open(file_path, 'a') as file:
            file.write(f"{alpha_id}\n")
        print(f"Added failed Alpha ID {alpha_id} to blacklist in real-time")
        return True
    except Exception as e:
        print(f"Error updating blacklist file in real-time: {e}")
        return False


def sign_in():
    username, password = read_credentials(args.credentials_file)
    if not username or not password:
        print("Unable to obtain valid username or password")
        return None

    s = requests.Session()
    s.auth = HTTPBasicAuth(username, password)
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    })

    while True:
        try:
            response = s.post('https://api.worldquantbrain.com/authentication')
            response.raise_for_status()
            auth_data = response.json()
            user_id = auth_data['user']['id']
            print(f"{user_id}, Authentication successful.")

            # If there's a token, add it to headers
            if 'token' in auth_data:
                s.headers.update({'Authorization': f'Bearer {auth_data["token"]}'})
            break
        except requests.HTTPError as e:
            print(f"HTTP error occurred: {e}. Retrying...")
            time.sleep(10)
        except Exception as e:
            print(f"Error during authentication: {e}. Trying to login again.")
            time.sleep(10)
    return s


def requests_wq(s, type='get', url='', json=None, t=15):
    session = s
    while True:
        try:
            if type == 'get':
                ret = session.get(url, timeout=(10, 30))
            elif type == 'post':
                if json is None:
                    ret = session.post(url, timeout=(10, 30))
                else:
                    ret = session.post(url, json=json, timeout=(10, 30))
            elif type == 'patch':
                ret = session.patch(url, json=json, timeout=(10, 30))
            else:
                raise ValueError(f"Unsupported request type: {type}")

            if ret.status_code == 429:
                print(f"Status={ret.status_code}, delay {t} seconds")
                time.sleep(t)
                continue
            if ret.status_code in (200, 201):
                return ret, session
            if ret.status_code == 401:
                print("Authentication expired, logging in again...")
                session = sign_in()
                if not session:
                    print("Re-login failed")
                    return None, None
                continue
            else:
                print(f"\033[31mStatus={ret.status_code}, continue\033[0m")
                continue
        except requests.RequestException as e:
            print(f"Error during method execution: {e}. Retrying...")
            time.sleep(10)
            session = sign_in()
            if not session:
                print("Re-login failed")
                return None, None
            print(f"Delay 10 seconds, reconnecting")
    return None, None


def set_alpha_properties(s, alpha_id, name: str = None, color: str = None,
                         selection_desc: str = "None", combo_desc: str = "None",
                         tags: str = "submitted", regular_desc: str = "None"):
    """
    Function changes alpha's description parameters
    """
    params = {
        "color": color,
        "name": name,
        "tags": [tags],
        "category": None,
        "regular": {"description": regular_desc},
        "combo": {"description": combo_desc},
        "selection": {"description": selection_desc},
    }
    response, sess = requests_wq(s, 'patch', f"https://api.worldquantbrain.com/alphas/{alpha_id}", params)
    return response, sess


# Check Alpha submission status (enhanced version)
def get_check_submission(s, alpha_id):
    sess = s
    for count_i in range(3):  # 3 attempts
        try:
            while True:
                result, sess = requests_wq(sess, 'get', f"https://api.worldquantbrain.com/alphas/{alpha_id}/check")
                if result is None:
                    return "error", sess

                if "retry-after" in result.headers:
                    time.sleep(float(result.headers["Retry-After"]))
                else:
                    break

            if result.json().get("is", 0) == 0:
                print(f"Alpha {alpha_id}: logged out, returning 'sleep'")
                if count_i < 2:  # Not the last retry
                    time.sleep(40)
                    continue
                return "sleep", sess

            checks_df = pd.DataFrame(result.json()["is"]["checks"])
            # Check if SELF_CORRELATION is "nan"
            self_correlation_value = checks_df[checks_df["name"] == "SELF_CORRELATION"]["value"].values[0]
            pc = self_correlation_value

            if any(checks_df["result"] == "ERROR"):
                print(f"Alpha {alpha_id}: \033[31m ERROR \033[0m, check failed")
                return "ERROR", sess
            if any(checks_df["result"] == "FAIL"):
                print(f"Alpha {alpha_id}: \033[31m FAIL \033[0m, check failed")
                return "FAIL", sess
            if pd.isna(self_correlation_value) or str(self_correlation_value).lower() == "nan":
                print(f"Alpha {alpha_id}: SELF_CORRELATION is \033[31m nan \033[0m, check failed")
                return "nan", sess
            else:
                print(f"\033[34m  Alpha {alpha_id}: check passed  \033[0m ")
                return pc, sess

        except Exception as e:
            print(f"Check exception: {alpha_id} - {str(e)}")
            if count_i < 2:  # Not the last retry
                time.sleep(10)
                continue
            return "error", sess

    return "timeout", sess


# Submit Alpha (enhanced version)
def submit_alpha(s, alpha_id):
    max_retries = 3
    retry_delay = 20
    status_code = None
    sess = s

    for retry in range(max_retries):
        if retry > 0:
            print(f"Connection issue, waiting {retry_delay} seconds before attempt {retry + 1}...")
            time.sleep(retry_delay)

        try:
            response, sess = requests_wq(sess, 'post', f"https://api.worldquantbrain.com/alphas/{alpha_id}/submit", {})
            if response is None:
                continue

            status_code = response.status_code
            print(f"Submission status code: {status_code}")

            if status_code < 300:
                return True, status_code, sess  # Successful submission
            elif status_code == 400:
                print(f"Alpha {alpha_id}: Status code 400 (Bad Request), submission failed")
                return False, status_code, sess  # Return failure, don't trigger blacklist
            elif status_code == 403:
                print(f"Alpha {alpha_id}: Status code 403 (Forbidden), submission failed")
                return False, status_code, sess  # Return failure, trigger blacklist
            elif status_code == 429:
                print(f"Rate limit triggered, waiting longer...")
                time.sleep(retry_delay * 2)
                continue

        except Exception as e:
            print(f"Submission error: {str(e)}")
            continue

    return False, status_code, sess  # Return after retry failure


# Get Alpha count for specific status
def get_alpha_count(s, status):
    sess = s
    try:
        url = f"https://api.worldquantbrain.com/users/self/alphas?limit=1&status={status}"
        response, sess = requests_wq(sess, 'get', url)
        if response and response.status_code < 300:
            count = response.json().get('count', 0)
            return count, sess
        else:
            print(f"Failed to get Alpha count for status '{status}'")
            return None, sess
    except Exception as e:
        print(f"Error getting Alpha count for status '{status}': {e}")
        return None, sess


# Get valid Alphas
def get_alphas(s, start_date, end_date, sharpe_th, fitness_th, turnover_th, region, alpha_num, usage):
    sess = s
    output = []
    count = 0
    current_year = datetime.now().strftime('%Y')

    for i in range(0, alpha_num, 100):
        print(f"Getting batch {i // 100 + 1} of Alphas...")

        # Modify URL, add fitness upper limit condition
        url_e = f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={i}" \
                f"&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E={current_year}-{start_date}" \
                f"T00:00:00-04:00&dateCreated%3C{current_year}-{end_date}" \
                f"T00:00:00-04:00&is.fitness%3E{fitness_th}&is.fitness%3C2.5&is.sharpe%3E{sharpe_th}" \
                f"&settings.region={region}&order=is.sharpe&hidden=false&type!=SUPER" \
                f"&is.turnover%3C{turnover_th}"

        # For negative values, use &is.fitness%3E-2.5 as lower limit
        url_c = f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={i}" \
                f"&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E={current_year}-{start_date}" \
                f"T00:00:00-04:00&dateCreated%3C{current_year}-{end_date}" \
                f"T00:00:00-04:00&is.fitness%3C-{fitness_th}&is.fitness%3E-2.5&is.sharpe%3C-{sharpe_th}" \
                f"&settings.region={region}&order=is.sharpe&hidden=false&type!=SUPER" \
                f"&is.turnover%3C{turnover_th}"

        urls = [url_e]
        if usage != "submit":
            urls.append(url_c)

        batch_empty = True  # Mark whether this batch has data

        for url in urls:
            response, sess = requests_wq(sess, 'get', url)
            if response is None:
                print(f"Failed to get batch {i // 100 + 1} Alphas, skipping")
                continue

            try:
                alpha_list = response.json()["results"]
                if len(alpha_list) == 0:
                    continue  # This URL has no data, try next URL

                batch_empty = False  # This batch has data
                print(f"Retrieved {len(alpha_list)} Alphas")

                for j in range(len(alpha_list)):
                    alpha_id = alpha_list[j]["id"]
                    if alpha_id in blacklist:
                        print(f"Skipping Alpha ID {alpha_id} because it's in the blacklist")
                        continue

                    name = alpha_list[j]["name"]
                    dateCreated = alpha_list[j]["dateCreated"]
                    sharpe = alpha_list[j]["is"]["sharpe"]
                    fitness = alpha_list[j]["is"]["fitness"]
                    turnover = alpha_list[j]["is"]["turnover"]
                    margin = alpha_list[j]["is"]["margin"]
                    longCount = alpha_list[j]["is"]["longCount"]
                    shortCount = alpha_list[j]["is"]["shortCount"]
                    decay = alpha_list[j]["settings"]["decay"]
                    exp = alpha_list[j]['regular']['code']
                    count += 1

                    checks = alpha_list[j].get("is", {}).get("checks", [])
                    has_failed_checks = any(check.get('result') == 'FAIL' for check in checks if check)
                    if has_failed_checks:
                        print(f"Skipping Alpha ID {alpha_id} because it has failed check items")
                        continue

                    if (longCount + shortCount) > 100 and turnover < turnover_th:
                        if sharpe < -sharpe_th:
                            exp = "-%s" % exp
                        rec = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
                        print(rec)

                        if turnover > 0.25:
                            rec.append(decay + 2)
                        elif turnover > 0.2:
                            rec.append(decay)
                        elif turnover > 0.15:
                            rec.append(decay)
                        else:
                            rec.append(decay)
                        output.append(rec)

            except Exception as e:
                print(f"Error processing batch {i // 100 + 1} Alphas: {e}")
                continue

        # If all URLs return no data, it means there are no more Alphas
        if batch_empty:
            print(f"Batch {i // 100 + 1} has no more data, stopping retrieval")
            break

    print(f"Total qualifying Alphas actually retrieved: {len(output)}")
    print(f"Total Alphas traversed: {count}")
    return output, sess


# Main program
def main():
    print("=== WorldQuant Alpha Submitter - Optimized Version ===")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Credentials file: {args.credentials_file}")
    print(f"Blacklist file: {args.blacklist_file}")
    print(f"Date range: {args.start_date} to {args.end_date}")
    print(f"Number of Alphas to check: {args.alpha_num}")
    print(f"Region: {args.region}")
    print(f"Sharpe threshold: {args.sharpe_th}")
    print(f"Fitness threshold: {args.fitness_th}")
    print(f"Turnover threshold: {args.turnover_th}")
    print(f"Submission delay: {args.submit_delay} seconds")
    print(f"Maximum allowed submitted Alpha change count: {args.max_submitted_change}")

    # Read credentials and blacklist
    username, password = read_credentials(args.credentials_file)
    if not username or not password:
        print("Unable to obtain valid username or password, please check credentials file format.")
        print("Credentials file should be in JSON format: [\"your_email@example.com\",\"your_password\"]")
        print("Or line-separated format: first line email, second line password")
        return

    global blacklist
    blacklist = read_blacklist(args.blacklist_file)

    # Login
    s = sign_in()
    if not s:
        print("Login failed, program exiting")
        return

    # Get initial submitted count
    initial_submitted_count, s = get_alpha_count(s, "ACTIVE")
    if initial_submitted_count is not None:
        print(f"Number of submitted Alphas on platform: {initial_submitted_count}")
    else:
        print("Cannot calculate ACTIVE, please check login credentials or network connection")

    print("\nGetting Alpha list...")
    print(
        f"\nSearching for valid alphas meeting criteria (Sharpe >= {args.sharpe_th}, Fitness >= {args.fitness_th}, Turnover < {args.turnover_th})...")

    valid_alphas_data, s = get_alphas(s, args.start_date, args.end_date, args.sharpe_th,
                                      args.fitness_th, args.turnover_th, args.region,
                                      args.alpha_num, "submit")

    valid_alphas = [alpha[0] for alpha in valid_alphas_data]
    alpha_metrics = {
        alpha[0]: {"exp": alpha[1], "sharpe": alpha[2], "turnover": alpha[3],
                   "fitness": alpha[4], "margin": alpha[5]}
        for alpha in valid_alphas_data
    }

    print(f"Found {len(valid_alphas)} valid Alphas (excluding failed check items and Alphas in blacklist)")

    if not valid_alphas:
        print("No valid Alphas meeting criteria found, no submission needed.")
        return

    print(f"\nPreparing to auto-submit {len(valid_alphas)} valid Alphas")
    submitted = 0
    failed = 0

    # First round of submission
    for i, alpha_id in enumerate(valid_alphas):
        print(f"\nChecking {i + 1}/{len(valid_alphas)}: {alpha_id}")
        print(f"[Sharpe: {alpha_metrics[alpha_id]['sharpe']}, Fitness: {alpha_metrics[alpha_id]['fitness']}, "
              f"Turnover: {alpha_metrics[alpha_id]['turnover']}, Margin: {alpha_metrics[alpha_id]['margin']}]")
        print(f"[exp: {alpha_metrics[alpha_id]['exp']}]")

        # Check Alpha status
        check_result, s = get_check_submission(s, alpha_id)
        print(f"alphaId={alpha_id}, check_result={check_result}")

        # Handle according to check result
        if check_result == "sleep":
            print(f"Alpha={alpha_id}: \033[33m Check result: sleep, skipping this Alpha (not adding to blacklist) \033[0m")
            failed += 1
            continue
        elif check_result in ("timeout", "error"):
            print(f"Alpha={alpha_id}: \033[33m Check result: {check_result}, network/system issue, not adding to blacklist temporarily, tagged, check Tag-timeout on platform and manually verify submission \033[0m")
            # Tag for subsequent manual check
            try:
                set_alpha_properties(s, alpha_id,
                                     name=datetime.now().strftime("%Y.%m.%d"),
                                     tags="timeout")
            except Exception as e:
                print(f"Failed to set Alpha tag: {e}")
            failed += 1
            continue
        elif check_result == "FAIL":
            print(f"Alpha={alpha_id}: \033[31m Check result: FAIL, Alpha doesn't meet requirements, adding to blacklist \033[0m")
            failed += 1
            if update_blacklist(args.blacklist_file, alpha_id):
                blacklist.add(alpha_id)
            continue
        elif check_result in ("nan", "ERROR"):
            print(f"Alpha={alpha_id}: \033[31m Check result: {check_result}, possibly Alpha issue, not adding to blacklist temporarily, tagged, check Tag-timeout on platform and manually verify submission \033[0m")
            # ERROR and nan are special, may be temporary issues, tag but don't blacklist immediately
            try:
                set_alpha_properties(s, alpha_id,
                                     name=datetime.now().strftime("%Y.%m.%d"),
                                     tags="timeout")
            except Exception as e:
                print(f"Failed to set Alpha tag: {e}")
            failed += 1
            continue
        else:
            print(f"Check result: \033[32mpassed\033[0m (SELF_CORRELATION: {check_result}), starting submission")

        # Submit Alpha
        success, status_code, s = submit_alpha(s, alpha_id)
        if success:
            print(f"Submission result: \033[32mSubmitted!\033[0m Status code: {status_code}")

            # Set Alpha tag
            try:
                set_alpha_properties(s, alpha_id,
                                     name=datetime.now().strftime("%Y.%m.%d"),
                                     tags="submitted")
            except Exception as e:
                print(f"Failed to set Alpha tag: {e}")

            if status_code == 201:
                if condition:
                    try:
                        winsound.MessageBeep()
                        winsound.Beep(1000, 500)
                    except:
                        pass  # Ignore sound playback errors

            submitted += 1
            delay = args.submit_delay + random.uniform(5, 15)
            print(f"Waiting {delay:.2f} seconds...")
            time.sleep(delay)

            # Check submitted count change
            current_submitted_count, s = get_alpha_count(s, "ACTIVE")
            if current_submitted_count is None:
                print("Unable to get current submitted Alpha count, continuing execution...")
            else:
                change = abs(current_submitted_count - initial_submitted_count)
                print(f"Total successful submissions: {change}!")
                if change >= args.max_submitted_change:
                    print(f"Warning: Change in submitted Alpha count ({change}) exceeds threshold ({args.max_submitted_change})!")
                    print(f"Expected submitted count: {submitted}, Actual submitted count: {change}")
                    print("Program stopping execution.")
                    return
        else:
            print(f"Submission result: \033[31mFailed!\033[0m Status code: {status_code}")
            failed += 1
            if status_code not in (400, 429):
                if update_blacklist(args.blacklist_file, alpha_id):
                    blacklist.add(alpha_id)

    print(f"\nFirst round submission:")
    print(f"Total: {len(valid_alphas)} Alphas")
    print(f"Submitted: {submitted}")
    print(f"Failed: {failed}")

    # Second round retry (optional)
    if failed > 0 and submitted < args.max_submitted_change:
        print(f"\nStarting re-check and submission of failed Alphas")
        retry_submitted = 0
        retry_failed = 0

        # Re-get Alpha list (excluding blacklist)
        valid_alphas_data, s = get_alphas(s, args.start_date, args.end_date, args.sharpe_th,
                                          args.fitness_th, args.turnover_th, args.region,
                                          args.alpha_num, "submit")
        valid_alphas = [alpha[0] for alpha in valid_alphas_data if alpha[0] not in blacklist]

        for i, alpha_id in enumerate(valid_alphas):
            if submitted + retry_submitted >= args.max_submitted_change:
                print("Reached maximum submission count limit, stopping retry")
                break

            print(f"Re-checking {i + 1}/{len(valid_alphas)}: {alpha_id}")

            # Check Alpha status
            check_result, s = get_check_submission(s, alpha_id)

            # Handle according to check result
            if check_result == "sleep":
                print(f"Alpha={alpha_id}: \033[33m Check result: sleep, skipping this Alpha (not adding to blacklist) \033[0m")
                retry_failed += 1
                continue
            elif check_result in ("timeout", "error"):
                print(f"Alpha={alpha_id}: \033[33m Check result: {check_result}, network/system issue, not adding to blacklist \033[0m")
                retry_failed += 1
                continue
            elif check_result in ("nan", "ERROR", "FAIL"):
                print(f"Alpha={alpha_id}: \033[31m Check result: {check_result}, Alpha quality issue, adding to blacklist \033[0m")
                retry_failed += 1
                if update_blacklist(args.blacklist_file, alpha_id):
                    blacklist.add(alpha_id)
                continue
            else:
                print(f"Check result: \033[32mpassed\033[0m (SELF_CORRELATION: {check_result}), starting submission")

            # Check submitted count
            current_submitted_count, s = get_alpha_count(s, "ACTIVE")
            if current_submitted_count is not None:
                change = abs(current_submitted_count - initial_submitted_count)
                if change >= args.max_submitted_change:
                    print(f"Reached maximum submission count limit, stopping execution")
                    return

            # Submit Alpha
            success, status_code, s = submit_alpha(s, alpha_id)
            if success:
                print(f"Submission result: \033[32mSubmitted!\033[0m Status code: {status_code}")
                retry_submitted += 1
                submitted += 1
                failed -= 1

                # Set Alpha tag
                try:
                    set_alpha_properties(s, alpha_id,
                                         name=datetime.now().strftime("%Y.%m.%d"),
                                         tags="submitted")
                except Exception as e:
                    print(f"Failed to set Alpha tag: {e}")

                delay = args.submit_delay + random.uniform(5, 15)
                print(f"Waiting {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                print(f"Submission result: \033[31mFailed!\033[0m Status code: {status_code}")
                retry_failed += 1
                if status_code not in (400, 429):
                    if update_blacklist(args.blacklist_file, alpha_id):
                        blacklist.add(alpha_id)

        print(f"\nSecond round submission:")
        print(f"Attempted re-submission: {len(valid_alphas)}")
        print(f"Re-submission successful: {retry_submitted}")
        print(f"Re-submission failed: {retry_failed}")

    # Final summary
    print(f"\nFinal results:")
    print(f"Total: {len(valid_alphas)} Alphas")
    print(f"Submitted: {submitted}")
    print(f"Failed: {failed}")
    if len(valid_alphas) > 0:
        print(f"Submission rate: {(submitted / len(valid_alphas) * 100):.2f}%")
    print(f"Completion time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    final_submitted_count, s = get_alpha_count(s, "ACTIVE")
    if final_submitted_count is not None and initial_submitted_count is not None:
        actual_increase = final_submitted_count - initial_submitted_count
        print(f"Successfully added new submissions this run: {actual_increase}")
        print(f"Program recorded submissions: {submitted}")


if __name__ == "__main__":
    main()