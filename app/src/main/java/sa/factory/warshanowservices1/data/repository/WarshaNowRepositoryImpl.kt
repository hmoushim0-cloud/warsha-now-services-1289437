package sa.factory.warshanowservices1.data.repository

import sa.factory.warshanowservices1.domain.repository.WarshaNowRepository
import sa.factory.warshanowservices1.hardware.HardwareSensorDriver
import sa.factory.warshanowservices1.model.WarshaNowItem
import sa.factory.warshanowservices1.model.WarshaNowOrder
import sa.factory.warshanowservices1.model.WarshaNowTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class WarshaNowRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : WarshaNowRepository {

    private val _catalog = MutableStateFlow<List<WarshaNowItem>>(listOf(
        WarshaNowItem("1", "فحص أمان مدى", "تحليل اختراق NFC EMV L2 مع تقارير امتثال PCI-DSS", 350.0, "معتمد", "فحص الأمان"),
        WarshaNowItem("2", "مطابقة واجهة فيجما", "تدقيق بكسل مثالي بين التصميم والتنفيذ مع تقرير تفاعلي", 220.0, "فوري", "مطابقة فيجما"),
        WarshaNowItem("3", "تحليل رحلة المستخدم", "تتبع GPS لسيناريوهات الخدمة الميدانية مع تحليل الاحتكاك", 180.0, "جديد", "اختبار الأداء")
    ))
    override val catalogFlow: Flow<List<WarshaNowItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<WarshaNowItem>>(emptyList())
    override val activeCart: StateFlow<List<WarshaNowItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<WarshaNowTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            WarshaNowTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: WarshaNowItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: WarshaNowItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): WarshaNowOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = WarshaNowOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
