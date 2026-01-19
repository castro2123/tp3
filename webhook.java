@RestController
@RequestMapping("/webhook")
public class WebhookController {

    @Value("${processor.url}")
    private String processorUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    @PostMapping("/status")
    public ResponseEntity<Void> receive(@RequestBody Map<String, Object> payload) {

        restTemplate.postForEntity(
            processorUrl + "/webhook/status",
            payload,
            Void.class
        );

        return ResponseEntity.ok().build();
    }
}
