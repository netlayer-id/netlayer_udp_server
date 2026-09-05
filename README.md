# NetLayer UDP Server

A robust, production-ready UDP server library for PHP with worker pool architecture, rate limiting, timer support, and comprehensive error handling.

[![PHP Version](https://img.shields.io/badge/PHP-%3E%3D7.0-blue.svg)](https://php.net)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Features

- **High Performance** - Non-blocking I/O with worker pool architecture
- **Auto Recovery** - Automatic worker respawn and socket recovery
- **Rate Limiting** - Built-in token bucket rate limiter
- **Timer Support** - Schedule recurring or one-time tasks
- **Statistics** - Real-time server statistics and monitoring
- **Thread Safe** - File-based locking for multi-process safety
- **Event Driven** - Flexible event system with multiple listeners
- **Auto Cleanup** - Memory monitoring and periodic maintenance
- **Comprehensive Logging** - Detailed logging with configurable output
- **Security** - Input validation and error isolation

## Requirements

- PHP 7.0 or higher
- PCNTL extension (for process forking)
- POSIX extension (for signal handling)
- Sockets extension (optional, for better performance)

### Check Extensions

```bash
php -m | grep pcntl
php -m | grep sockets
```

### Install PCNTL (Ubuntu/Debian)

```bash
sudo apt-get install php-pcntl
sudo service php-fpm restart
```

## Installation

### Manual

```bash
git clone https://github.com/netlayer-id/netlayer_udp_server.git
cd netlayer_udp_server
```

### Include in your project

```php
require_once 'path/to/netlayer.php';
```

## Quick Start

```php
<?php
require 'netlayer.php';

use netlayer\server;

$server = new server('0.0.0.0', 8080);

$server->on('start', function($server) {
    echo "Server started on {$server->getHost()}:{$server->getPort()}\n";
});

$server->on('packet', function($server, $data, $client, $workerId, $pid) {
    echo "Packet from {$client['address']}:{$client['port']}\n";
    $server->sendTo($client['address'], $client['port'], "Echo: {$data}");
});

$server->start();
```

## Configuration

```php
$server = new server('0.0.0.0', 8080, [
    'max_packet_size' => 8192,
    'num_workers' => 4,
    'max_queue_size' => 1000,
    'rate_limit' => 100,
    'rate_burst' => 200,
    'worker_heartbeat_timeout' => 30,
    'queue_file' => '/tmp/netlayer_queue',
    'log_file' => '/var/log/netlayer.log',
    'max_idle_time' => 0,
    'debug' => false,
    'socket_timeout_usec' => 100000,
    'max_consecutive_errors' => 1000,
    'maintenance_interval' => 60,
    'heartbeat_interval' => 5
]);
```

## Events

### Server Events

| Event | Description | Parameters |
|-------|-------------|------------|
| start | Server started | server |
| stop | Server stopped | server |
| packet | Packet received | server, data, client, workerId, pid |
| queue_full | Queue is full | server, data, client |
| rate_limited | Client rate limited | server, client |
| workers_started | Workers started | server, count |
| worker_exit | Worker exited | server, pid, workerId, duration |
| worker_respawned | Worker respawned | server, pid, workerId |
| worker_error | Worker error | server, workerId, pid, error |
| workers_stopped | All workers stopped | server |
| socket_restarted | Socket restarted | server |
| heartbeat | Heartbeat tick | server, stats |
| high_memory | High memory usage | server, usage, limit |
| reload | Reload signal | server |

### Example Usage

```php
$server->on('packet', function($server, $data, $client, $workerId, $pid) {
    echo "Packet from {$client['address']}:{$client['port']}\n";
    echo "Data: {$data}\n";
    echo "Worker ID: {$workerId}\n";
    echo "Process ID: {$pid}\n";
});

$server->on('rate_limited', function($server, $client) {
    echo "Rate limited: {$client['address']}\n";
});

$server->on('worker_error', function($server, $workerId, $pid, $error) {
    echo "Worker {$workerId} error: {$error->getMessage()}\n";
});
```

## Timer Usage

```php
use netlayer\timer;

// Recurring timer (every 1 second)
$timerId = timer::tick(1000, function() {
    echo "This runs every second\n";
});

// One-time timer (after 5 seconds)
timer::after(5000, function() {
    echo "This runs once after 5 seconds\n";
});

// Clear specific timer
timer::clear($timerId);

// Clear all timers
timer::clearAll();

// Get active timer count
$count = timer::getActiveTimerCount();
```

## Statistics

```php
$stats = $server->getStatistics();
print_r($stats);
```

Output:

```php
Array
(
    [packets_received] => 1000
    [packets_processed] => 995
    [packets_dropped] => 5
    [bytes_received] => 8192000
    [queue_max_size] => 50
    [rate_limited] => 0
    [worker_restarts] => 0
    [errors] => 0
    [socket_errors] => 0
    [queue_size] => 0
    [active_workers] => 4
    [uptime] => 3600
    [memory_usage] => 2097152
    [peak_memory_usage] => 4194304
    [rate_limited_clients] => 0
    [active_timers] => 1
)
```

## Worker Management

```php
$workerStatus = $server->getWorkerStatus();

foreach ($workerStatus as $worker) {
    echo "PID: {$worker['pid']}\n";
    echo "ID: {$worker['id']}\n";
    echo "Uptime: {$worker['uptime']} seconds\n";
    echo "Last Heartbeat: {$worker['last_heartbeat']}\n";
}
```

## Rate Limiting

```php
$server = new server('0.0.0.0', 8080, [
    'rate_limit' => 50,
    'rate_burst' => 100
]);

$server->on('rate_limited', function($server, $client) {
    echo "Client rate limited: {$client['address']}\n";
});
```

## Running in Background

### Using nohup

```bash
nohup php server.php > server.log 2>&1 &
```

### Using systemd

Create service file:

```bash
sudo nano /etc/systemd/system/udpserver.service
```

Content:

```ini
[Unit]
Description=UDP Server
After=network.target

[Service]
Type=simple
User=www-data
WorkingDirectory=/path/to/server
ExecStart=/usr/bin/php server.php
Restart=always
RestartSec=5
StandardOutput=append:/var/log/udpserver.log
StandardError=append:/var/log/udpserver.log

[Install]
WantedBy=multi-user.target
```

Start service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable udpserver
sudo systemctl start udpserver
```

## Testing

Send UDP packet using netcat:

```bash
echo "test" | nc -u localhost 8080
```

Send UDP packet using PHP:

```php
<?php
$socket = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
socket_sendto($socket, 'test', 4, 0, '127.0.0.1', 8080);
socket_close($socket);
```

## License

MIT License

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request
