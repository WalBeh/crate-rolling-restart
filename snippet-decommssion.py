    def execute_decommission(self, pod_name: str, namespace: str, metrics: PodOperationMetrics) -> bool:
        """Execute decommission commands for a CrateDB pod."""
        pod_suffix = pod_name.rsplit("-", 1)[-1]

        timeout_value = "720s"
        force_value = True
        min_availability = "PRIMARIES"
        loop_message = "wait for crate to exit..."

        metrics.decommission_start = datetime.datetime.now()  # Use the passed metrics object

        # Load namespace-specific configuration
        namespace_config = MaintenancePlan.load_namespace_config()

        # Apply namespace-specific configuration if available
        if namespace in namespace_config:
            metrics.min_availability = namespace_config[namespace]["min_availability"]
            metrics.timeout_value = namespace_config[namespace]["timeout_value"]
            config = namespace_config[namespace]
            min_availability = config["min_availability"]
            timeout_value = config["timeout_value"]
            maintenance_window = config.get("maintenance_window", {})
            logger.warning(f"{mode}Using custom settings for namespace {namespace}: min_availability={min_availability}, timeout={timeout_value}")

            in_maintenance = MaintenancePlan.is_in_maintenance_window(maintenance_window)

            if maintenance_window and not in_maintenance:
                logger.warning(f"{mode}Outside maintenance window {namespace}: Maintenance Window={maintenance_window}")

                retry_count = 6 * 24 * 2  # 2 days
                for attempt in range(retry_count):
                    wait_time_sec = 60 * 10  # 10 minutes
                    logger.warning(f"{mode}Waiting for 10 minutes before retry {attempt + 1}/{retry_count}...")
                    interactive_pause(
                        wait_time_sec,
                        prompt=f"Waiting for maintenance window (attempt {attempt + 1}/{retry_count})...",
                    )

                    # Re-check the maintenance window after waiting
                    in_maintenance = CratePodDrainer.is_in_maintenance_window(maintenance_window)
                    if in_maintenance:
                        logger.info(f"{mode}Now within maintenance window {namespace}")
                        break
                    logger.warning(f"{mode}Still outside maintenance window")

                if not in_maintenance:
                    logger.error(f"{mode}Outside maintenance window after {retry_count} attempts.")
                    sys.exit(1)
            else:
                if in_maintenance:
                    logger.info(f"{mode}Within maintenance window {namespace}: Maintenance Window={maintenance_window}")

        # Create JSON payloads for the SQL commands
        json_payload_new_primaries = json.dumps({"stmt": 'set global transient "cluster.routing.allocation.enable" = "new_primaries"'})
        json_payload_timeout = json.dumps({"stmt": f'set global transient "cluster.graceful_stop.timeout" = "{timeout_value}"'})
        json_payload_force = json.dumps({"stmt": f'set global transient "cluster.graceful_stop.force" = {str(force_value).lower()}'})
        json_payload_min_availability = json.dumps({"stmt": f'set global transient "cluster.graceful_stop.min_availability" = "{min_availability}"'})
        json_payload_decommission = json.dumps({"stmt": f"alter cluster decommission $$data-hot-{pod_suffix}$$"})

        logger.info(f"{mode}Decommissioning pod with command: {json_payload_decommission}")

        # Define the commands to execute
        commands = [
            f"curl --insecure -sS -H \"Content-Type: application/json\" -X POST https://127.0.0.1:4200/_sql -d '{json_payload_new_primaries}'",
            f"curl --insecure -sS -H \"Content-Type: application/json\" -X POST https://127.0.0.1:4200/_sql -d '{json_payload_timeout}'",
            f"curl --insecure -sS -H \"Content-Type: application/json\" -X POST https://127.0.0.1:4200/_sql -d '{json_payload_force}'",
            f"curl --insecure -sS -H \"Content-Type: application/json\" -X POST https://127.0.0.1:4200/_sql -d '{json_payload_min_availability}'",
            f"""
            curl_output=$(curl --insecure -sS -H "Content-Type: application/json" -X POST https://127.0.0.1:4200/_sql -d \'{json_payload_decommission}\')
            curl_exit_code=$?
            echo "DECOMMISSION_EXIT_CODE: $curl_exit_code"
            echo "DECOMMISSION_RESPONSE: $curl_output"
            if [ $curl_exit_code -eq 0 ]; then
                while kill -0 1 2>/dev/null; do
                    echo {loop_message}
                    sleep 0.5
                done
            else
                echo "Decommission command failed with exit code $curl_exit_code"
            fi
            """,
        ]

        if self.options.dry_run:
            logger.info(f"{mode}[DRY RUN] Would execute decommission commands on pod {pod_name}")
            return True

        exec_cnt = 0
        try:
            for idx, cmd in enumerate(commands):
                exec_cnt = idx + 1
                start_time = time.time()
                logger.debug(f"{mode}Executing command {exec_cnt}: {cmd}")

                # Use a reasonable timeout for each command - longer for the decommission command
                timeout = (
                    1200 if exec_cnt == 5 else 60  #
                )  # 20 minutes for decommission, 1 minute for others //TODO: ????
                status, resp = self.execute_with_timeout(pod_name, namespace, cmd, timeout)

                elapsed = time.time() - start_time

                if status == "timeout":
                    logger.warning(f"{mode}Command {exec_cnt} timed out after {elapsed:.2f}s: {resp}")
                    if exec_cnt == 5:
                        # For the decommission command, a timeout might still mean success
                        logger.info(f"{mode}Decommission command timed out, but it may still be proceeding")
                        return True
                    else:
                        # For other commands, a timeout is likely an issue
                        logger.error(f"{mode}Command {exec_cnt} failed with timeout")
                        return False
                elif status == "error":
                    if exec_cnt < 5:  # decommission statement not fired yet
                        logger.error(f"{mode}Command {exec_cnt} failed with error: {resp}")
                        return False
                    else:
                        # For decommission command, some errors might be due to connection loss during successful decommission
                        logger.warning(f"{mode}Decommission command failed with error: {resp}")
                        logger.warning(f"{mode}This might be due to the pod restarting during decommission, which could be normal")
                        return True
                else:
                    # Process the response based on which command was executed
                    if exec_cnt == 5:  # This is the decommission command
                        exit_code = None
                        decom_response = None
                        waiting_count = 0
                        other_messages = []

                        # Parse the output to extract structured information
                        for line in resp.splitlines():
                            if "DECOMMISSION_EXIT_CODE:" in line:
                                try:
                                    exit_code = int(line.split(":", 1)[1].strip())
                                except (ValueError, IndexError):
                                    exit_code = -1
                            elif "DECOMMISSION_RESPONSE:" in line:
                                decom_response = line.split(":", 1)[1].strip() if ":" in line else line
                            elif loop_message in line:
                                waiting_count += 1
                            elif line.strip():
                                other_messages.append(line)

                        # Log the decommission status with all captured info
                        logger.info(f"{mode}Command ({exec_cnt}) results for pod {pod_name}:")
                        logger.info(f"{mode} - Exit code: {exit_code if exit_code is not None else 'N/A'}")
                        logger.info(f"{mode} - Response: {decom_response or 'Empty response'}")
                        logger.info(f"{mode} - Wait messages: {waiting_count}x '{loop_message}'")

                        if other_messages:
                            logger.info(f"{mode} - Other output: {' | '.join(other_messages)}")

                        logger.info(f"{mode}Command executed in {elapsed:.2f} seconds")
                        metrics.decommission_total_time = f"{elapsed:.2f}"
                        metrics.waiting_count = waiting_count

                        # If we have an exit code, check it
                        if exit_code is not None and exit_code != 0:
                            logger.warning(f"{mode}Decommission command returned non-zero exit code: {exit_code}")
                            # Still return True because the command executed, even if it failed
                            # The pod may still be decommissioned successfully
                    else:
                        # For other commands, log the full response
                        logger.info(f"{mode}Command ({exec_cnt}) output {pod_name}: {resp} (Executed in {elapsed:.2f} seconds)")

            # If all commands executed successfully
            metrics.decommission_end = datetime.datetime.now()
            logger.opt(colors=True).info(f"{mode}<green>Decommission command {exec_cnt} completed for {pod_name}</green>")
            return True

        except Exception as e:
            elapsed = time.time() - start_time
            if exec_cnt < 5:  # decommission statement not fired yet
                logger.error(f"{mode}Exec failed on {exec_cnt} before decommission run for pod {pod_name} in {elapsed:.2f} seconds: {e}")
                return False
            else:
                logger.warning(f"{mode}Exception during decommission for pod {pod_name}, but command may still succeed: {e}")
                return True
